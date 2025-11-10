use anyhow::Result;
use config::ConfigError;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt};
use generic_builders::immutable::Builder;
use k8s_openapi::api::core::v1::ObjectReference;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::runtime::{watcher, Config, Controller};
use kube::{Api, Client, ResourceExt};
use kube_operator_util::status::{set_error, set_ready};
use kube_operator_util::util::watch_namespaces;
use log::{error, info};
use mongodb::action::CreateCollection;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{to_document, Bson, DateTime, Document};
use mongodb::options::{
    ChangeStreamPreAndPostImages, IndexOptions, Sphere2DIndexVersion, TextIndexVersion,
    TimeseriesGranularity,
};
use mongodb::{options, Collection, Database, IndexModel};
use resource::Direction::{Ascending, Descending};
use resource::IndexType::{Hashed, Text, TwoDimensional, TwoDimensionalSphere};
use resource::{
    Collation, CollationAlternate, CollationCaseFirst, CollationMaxVariable, CollationStrength,
    Direction, Granularity, IndexType, Key, Options, TimeSeries, ValidationAction, ValidationLevel,
    WildcardProjection,
};
use ::resource::{Index, MongoCollection};
use rustls::crypto::ring::default_provider;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::vec::Vec;
use thiserror::Error;
use tokio::time::sleep;

const BACK_OFF: Duration = Duration::from_secs(5);
const CLUSTERED_NAME: &str = "_id_";
const CONFIG_DATABASE: &str = "database";
const CONFIG_FILE: &str = "CONFIG_FILE";
const CONFIG_URL: &str = "url";
const CONTROLLER: &str = "mongo-collections";
const DEFAULT_CONFIG_FILE: &str = "conf/application";
const INTERVAL: Duration = Duration::from_secs(60);

type Entry<'a, T> = (&'a String, &'a T);

struct Data {
    client: Client,
    database: Database,
    recorder: Recorder,
}

struct MongoConfig {
    database: String,
    url: String,
}

#[derive(Error, Debug)]
enum OperatorError {
    #[error("the keys {0} have both the fields direction and indexType set")]
    InvalidKeys(String),
    #[error("MongoDB error: {0}")]
    MongoDB(#[from] mongodb::error::Error),
    #[error("kube API error")]
    Kube(#[from] kube::Error),
    #[error("the status of {0} could not be updated")]
    StatusPatch(String),
}

fn all_entries<T>(_: &Entry<T>) -> bool {
    true
}

fn any_text_index(s: &[Key]) -> bool {
    s.iter().any(is_text_index)
}

fn bson_entry_to_key(entry: Entry<Bson>) -> Option<Key> {
    match entry.1 {
        Bson::Int32(v) => Some(Key {
            field: entry.0.clone(),
            direction: direction(*v),
            index_type: None,
        }),
        Bson::String(v) => Some(Key {
            field: entry.0.clone(),
            direction: None,
            index_type: index_type(v),
        }),
        _ => None,
    }
}

fn bson_to_value(bson: &Bson) -> Value {
    match bson {
        Bson::Array(v) => json!(v),
        Bson::Boolean(v) => json!(v),
        Bson::DateTime(v) => date_time_to_value(v),
        Bson::Double(v) => json!(v),
        Bson::Document(v) => Value::from(document_to_json_map(v)),
        Bson::Int32(v) => json!(v),
        Bson::Int64(v) => json!(v),
        Bson::Null => json!(null),
        Bson::ObjectId(v) => object_id_to_value(v),
        Bson::String(v) => json!(v),
        _ => json!(null),
    }
}

fn bson_to_weight(bson: &Bson) -> u32 {
    match bson {
        Bson::Int32(v) => v.cast_unsigned(),
        Bson::Int64(v) => v.cast_unsigned() as u32,
        _ => 0,
    }
}

fn bson_to_wildcard_projection(bson: &Bson) -> WildcardProjection {
    match bson {
        Bson::Int32(v) => value_to_wildcard_projection(v.cast_unsigned()),
        Bson::Int64(v) => value_to_wildcard_projection(v.cast_unsigned() as u32),
        _ => WildcardProjection::Exclude,
    }
}

fn collation_to_model(c: &Collation) -> options::Collation {
    options::Collation::builder()
        .alternate(collation_alternate_to_model(c.alternate.clone()))
        .backwards(c.backwards)
        .case_first(collation_case_first_to_model(c.case_first.clone()))
        .case_level(c.case_level)
        .locale(c.locale.clone())
        .max_variable(collation_max_variable_to_model(c.max_variable.clone()))
        .normalization(c.normalization)
        .numeric_ordering(c.numeric_ordering)
        .strength(collation_strength_to_model(c.strength.clone()))
        .build()
}

fn collation_alternate_to_model(a: CollationAlternate) -> options::CollationAlternate {
    match a {
        CollationAlternate::NonIgnorable => options::CollationAlternate::NonIgnorable,
        CollationAlternate::Shifted => options::CollationAlternate::Shifted,
    }
}

fn collation_case_first_to_model(c: CollationCaseFirst) -> options::CollationCaseFirst {
    match c {
        CollationCaseFirst::Lower => options::CollationCaseFirst::Lower,
        CollationCaseFirst::Off => options::CollationCaseFirst::Off,
        CollationCaseFirst::Upper => options::CollationCaseFirst::Upper,
    }
}

fn collation_max_variable_to_model(c: CollationMaxVariable) -> options::CollationMaxVariable {
    match c {
        CollationMaxVariable::Punct => options::CollationMaxVariable::Punct,
        CollationMaxVariable::Space => options::CollationMaxVariable::Space,
    }
}

fn collation_strength_to_model(c: CollationStrength) -> options::CollationStrength {
    match c {
        CollationStrength::Identical => options::CollationStrength::Identical,
        CollationStrength::Primary => options::CollationStrength::Primary,
        CollationStrength::Quaternary => options::CollationStrength::Quaternary,
        CollationStrength::Secondary => options::CollationStrength::Secondary,
        CollationStrength::Tertiary => options::CollationStrength::Tertiary,
    }
}

fn collection_name(obj: &MongoCollection) -> &str {
    obj.spec
        .name
        .as_ref()
        .map_or_else(|| obj.metadata.name.as_ref().map_or("", |n| &n), |n| &n)
}

fn config() -> Result<config::Config, ConfigError> {
    config::Config::builder()
        .add_source(config::File::with_name(&config_filename()))
        .build()
}

fn config_filename() -> String {
    match env::var_os(CONFIG_FILE) {
        Some(v) => v
            .to_str()
            .map_or(DEFAULT_CONFIG_FILE.to_string(), |s| s.to_string()),
        None => DEFAULT_CONFIG_FILE.to_string(),
    }
}

async fn create_collection(
    name: &str,
    obj: &MongoCollection,
    database: &Database,
) -> Result<(), mongodb::error::Error> {
    info!("Create collection {}", name);

    Builder::new(database.create_collection(name))
        .update(|c| c.capped(obj.spec.capped.unwrap_or(false)))
        .update_if_some(
            |_| obj.spec.change_stream_pre_and_post_images,
            |c, ch| {
                c.change_stream_pre_and_post_images(
                    ChangeStreamPreAndPostImages::builder().enabled(*ch).build(),
                )
            },
        )
        .update_if_some(
            |_| obj.spec.clustered,
            |c, _| c.clustered_index(options::ClusteredIndex::default()),
        )
        .update_if_some(
            |_| obj.spec.collation.as_ref(),
            |c, v| c.collation(collation_to_model(v)),
        )
        .update_if_some(
            |_| obj.spec.expire_after_seconds,
            |c, v| c.expire_after_seconds(Duration::from_secs(*v)),
        )
        .update_if_some(|_| obj.spec.max, |c, v| c.max(*v))
        .update_if_some(|_| obj.spec.size, |c, v| c.size(*v))
        .update_if_some(
            |_| obj.spec.time_series.clone(),
            |c, v| c.timeseries(time_series(v)),
        )
        .update_if_some(|_| obj.spec.validator.clone(), set_validator)
        .update_if_some(
            |_| obj.spec.validation_action.clone(),
            |c, v| c.validation_action(validation_action(v.clone())),
        )
        .update_if_some(
            |_| obj.spec.validation_level.clone(),
            |c, v| c.validation_level(validation_level(v.clone())),
        )
        .build()
        .await
}

async fn create_index(
    collection: &Collection<Document>,
    index: &Index,
) -> Result<(), mongodb::error::Error> {
    collection
        .create_index(index_to_model(index))
        .await
        .map(|r| {
            info!(
                "Created index {} for collection {}",
                r.index_name,
                collection.name()
            );
        })
}

async fn create_new_indexes(
    collection: &Collection<Document>,
    specified: &[Index],
    found: &[Index],
) -> Result<bool, mongodb::error::Error> {
    let mut has_any = false;
    let indexes = specified.iter().filter(|i| !found.contains(i));

    for i in indexes {
        has_any = true;

        info!(
            "Creating index {} for collection {}",
            index_name(&i),
            collection.name()
        );

        create_index(collection, &i).await?;
    }

    Ok(has_any)
}

fn date_time_to_value(d: &DateTime) -> Value {
    d.try_to_rfc3339_string()
        .ok()
        .map_or(json!(null), |s| json!(s))
}

fn direction(v: i32) -> Option<Direction> {
    match v {
        -1 => Some(Descending),
        1 => Some(Ascending),
        _ => None,
    }
}

fn document_to_json_map(document: &Document) -> Map<String, Value> {
    document.iter().fold(Map::new(), |mut m, e| {
        m.insert(e.0.clone(), bson_to_value(e.1));
        m
    })
}

fn document_to_keys(keys: &Document, options: Option<&Options>) -> Vec<Key> {
    let original: Vec<Key> = keys.iter().filter_map(bson_entry_to_key).collect();

    options
        .filter(|_| any_text_index(&original))
        .and_then(text_index_keys)
        .unwrap_or(original)
}

fn document_to_map<T, M, P>(document: &Document, mapper: M, predicate: P) -> BTreeMap<String, T>
where
    M: Fn(&Bson) -> T,
    P: Fn(&Entry<Bson>) -> bool,
{
    document
        .iter()
        .filter(predicate)
        .fold(BTreeMap::new(), |mut m, e| {
            m.insert(e.0.clone(), mapper(e.1));
            m
        })
}

async fn drop_not_specified(
    collection: &Collection<Document>,
    specified: &[Index],
    found: &[Index],
) -> Result<bool, mongodb::error::Error> {
    let mut has_any = false;
    let names = found
        .iter()
        .filter(|i| !specified.contains(*i))
        .flat_map(|i| i.options.clone())
        .flat_map(|o| o.name);

    for n in names {
        has_any = true;
        info!("Dropping index {} of collection {}", n, collection.name());
        collection.drop_index(n).await?
    }

    Ok(has_any)
}

fn error_policy(_obj: Arc<MongoCollection>, _err: &OperatorError, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

fn event(error: &OperatorError) -> Event {
    Event {
        type_: EventType::Warning,
        reason: "Error".to_string(),
        note: Some(error.to_string()),
        action: "update".to_string(),
        secondary: None,
    }
}

async fn exists(database: &Database, collection: &str) -> Result<bool, mongodb::error::Error> {
    let names = database.list_collection_names().await?;

    Ok(names.iter().any(|n| n == collection))
}

fn index_model_to_index(index_model: &IndexModel) -> Index {
    let options = index_model.options.clone().map(model_to_options);

    Index {
        keys: document_to_keys(&index_model.keys, options.as_ref()),
        options,
    }
}

fn index_models_to_indexes(index_models: &[IndexModel]) -> Vec<Index> {
    index_models
        .iter()
        .map(index_model_to_index)
        .filter(is_not_clustered)
        .collect()
}

fn index_to_model(index: &Index) -> IndexModel {
    IndexModel::builder()
        .keys(keys_to_document(index.keys.as_slice()))
        .options(index.options.as_ref().map(options_to_model))
        .build()
}

fn index_type(v: &str) -> Option<IndexType> {
    match v {
        "hashed" => Some(Hashed),
        "text" => Some(Text),
        "2d" => Some(TwoDimensional),
        "2dsphere" => Some(TwoDimensionalSphere),
        _ => None,
    }
}

fn index_name(index: &Index) -> String {
    index
        .options
        .as_ref()
        .and_then(|o| o.name.clone())
        .unwrap_or("".to_string())
}

fn invalid_key(key: &&Key) -> bool {
    key.direction.is_some() && key.index_type.is_some()
}

fn invalid_keys(indexes: Option<&[Index]>) -> Vec<String> {
    indexes
        .iter()
        .flat_map(|i| *i)
        .flat_map(|i| i.keys.iter())
        .filter(invalid_key)
        .map(|k| k.field.clone())
        .collect()
}

fn is_not_clustered(index: &Index) -> bool {
    index
        .options
        .as_ref()
        .and_then(|o| o.name.clone())
        .is_none_or(|n| n != CLUSTERED_NAME)
}

fn is_not_ready(obj: &MongoCollection) -> bool {
    obj.status.is_some() && obj.status.as_ref().filter(|s| s.is_ready()).is_none()
}

fn is_text_index(key: &Key) -> bool {
    matches!(key.index_type, Some(IndexType::Text))
}

fn is_weight(entry: &Entry<Bson>) -> bool {
    matches!(entry.1, Bson::Int32(_) | Bson::Int64(_))
}

fn is_wildcard_projection(entry: &Entry<Bson>) -> bool {
    match entry.1 {
        Bson::Int32(v) => *v == 0 || *v == 1,
        Bson::Int64(v) => *v == 0 || *v == 1,
        _ => false,
    }
}

fn key_to_bson(key: &Key) -> Bson {
    match key.direction {
        Some(Ascending) => Bson::from(1),
        Some(Descending) => Bson::from(-1),
        None => match key.index_type {
            Some(Hashed) => Bson::from("hashed"),
            Some(Text) => Bson::from("text"),
            Some(TwoDimensional) => Bson::from("2d"),
            Some(TwoDimensionalSphere) => Bson::from("2dsphere"),
            None => Bson::Null,
        },
    }
}

fn keys_to_document(keys: &[Key]) -> Document {
    let mut document = Document::new();

    keys.iter().for_each(|k| {
        document.insert(k.field.clone(), key_to_bson(k));
    });

    document
}

async fn list_indexes(collection: &Collection<Document>) -> Result<Vec<Index>, OperatorError> {
    let cursor = collection.list_indexes().await?;
    let result: Vec<IndexModel> = cursor.try_collect().await?;

    Ok(index_models_to_indexes(result.as_slice()))
}

#[tokio::main]
async fn main() -> Result<()> {
    const VERSION: &str = "1.0.0";

    env_logger::init();
    default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let config = config()?;
    let mongo_config = mongo_config(&config)?;
    let mongo_client: mongodb::Client = mongodb::Client::with_uri_str(&mongo_config.url).await?;
    let client = Client::try_default().await?;

    info!("Version: {VERSION}");

    join_all(
        watch(client.clone())
            .iter()
            .map(|c| {
                Controller::new(c.clone(), watcher::Config::default())
                    .with_config(Config::default().concurrency(1))
                    .shutdown_on_signal()
                    .run(
                        reconcile,
                        error_policy,
                        Arc::new(Data {
                            client: client.clone(),
                            database: mongo_client.database(&mongo_config.database),
                            recorder: Recorder::new(
                                client.clone(),
                                Reporter {
                                    controller: CONTROLLER.to_string(),
                                    instance: None,
                                },
                            ),
                        }),
                    )
                    .for_each(|res| async move {
                        match res {
                            Ok(o) => info!("Reconciled {o:?}"),
                            Err(e) => error!("Reconciliation failed: {}", source_message(&e)),
                        }
                    })
            })
            .collect::<Vec<_>>(),
    )
    .await;

    Ok(())
}

fn map_to_document<T, M, P>(map: &BTreeMap<String, T>, mapper: M, predicate: P) -> Document
where
    M: Fn(&T) -> Bson,
    P: Fn(&Entry<T>) -> bool,
{
    map.iter()
        .filter(predicate)
        .fold(Document::new(), |mut d, e| {
            d.insert(e.0.clone(), mapper(e.1));
            d
        })
}

fn model_to_collation(collation: options::Collation) -> Collation {
    Collation {
        alternate: model_to_collation_alternate(collation.alternate),
        backwards: collation
            .backwards
            .map_or_else(Collation::default_backwards, |b| b),
        case_first: model_to_collation_case_first(collation.case_first),
        case_level: collation
            .backwards
            .map_or_else(Collation::default_case_level, |b| b),
        locale: collation.locale,
        max_variable: model_to_collation_max_variable(collation.max_variable),
        normalization: collation
            .backwards
            .map_or_else(Collation::default_normalization, |b| b),
        numeric_ordering: collation
            .backwards
            .map_or_else(Collation::default_numeric_ordering, |b| b),
        strength: model_to_collation_strength(collation.strength),
    }
}

fn model_to_collation_alternate(a: Option<options::CollationAlternate>) -> CollationAlternate {
    match a {
        Some(options::CollationAlternate::Shifted) => CollationAlternate::Shifted,
        Some(options::CollationAlternate::NonIgnorable) => CollationAlternate::NonIgnorable,
        _ => Collation::default_alternate(),
    }
}

fn model_to_collation_case_first(c: Option<options::CollationCaseFirst>) -> CollationCaseFirst {
    match c {
        Some(options::CollationCaseFirst::Lower) => CollationCaseFirst::Lower,
        Some(options::CollationCaseFirst::Off) => CollationCaseFirst::Off,
        Some(options::CollationCaseFirst::Upper) => CollationCaseFirst::Upper,
        _ => Collation::default_case_first(),
    }
}

fn model_to_collation_max_variable(
    m: Option<options::CollationMaxVariable>,
) -> CollationMaxVariable {
    match m {
        Some(options::CollationMaxVariable::Punct) => CollationMaxVariable::Punct,
        Some(options::CollationMaxVariable::Space) => CollationMaxVariable::Space,
        _ => Collation::default_max_variable(),
    }
}

fn model_to_collation_strength(s: Option<options::CollationStrength>) -> CollationStrength {
    match s {
        Some(options::CollationStrength::Identical) => CollationStrength::Identical,
        Some(options::CollationStrength::Primary) => CollationStrength::Primary,
        Some(options::CollationStrength::Quaternary) => CollationStrength::Quaternary,
        Some(options::CollationStrength::Secondary) => CollationStrength::Secondary,
        Some(options::CollationStrength::Tertiary) => CollationStrength::Tertiary,
        _ => Collation::default_strength(),
    }
}

fn model_to_options(options: IndexOptions) -> Options {
    Options {
        bits: options.bits,
        collation: options.collation.map(model_to_collation),
        default_language: options.default_language,
        expire_after_seconds: options.expire_after.map(|d| d.as_secs()),
        hidden: options.hidden,
        language_override: options.language_override,
        max: options.max,
        min: options.min,
        name: options.name,
        partial_filter_expression: options
            .partial_filter_expression
            .map(|d| document_to_map(&d, bson_to_value, all_entries)),
        sparse: options.sparse,
        sphere_index_version: options
            .sphere_2d_index_version
            .map(sphere_index_version_to_number),
        text_index_version: options.text_index_version.map(text_index_version_to_number),
        unique: options.unique,
        weights: options
            .weights
            .map(|d| document_to_map(&d, bson_to_weight, is_weight)),
        wildcard_projection: options
            .wildcard_projection
            .map(|d| document_to_map(&d, bson_to_wildcard_projection, is_wildcard_projection)),
    }
}

fn mongo_config(c: &config::Config) -> Result<MongoConfig, ConfigError> {
    Ok(MongoConfig {
        url: c.get_string(CONFIG_URL)?,
        database: c.get_string(CONFIG_DATABASE)?,
    })
}

fn name(s: &Option<String>) -> &str {
    s.as_ref().map_or("", |n| n)
}

fn number_to_sphere_index_version(version: u32) -> Sphere2DIndexVersion {
    match version {
        2 => Sphere2DIndexVersion::V2,
        3 => Sphere2DIndexVersion::V3,
        v => Sphere2DIndexVersion::Custom(v),
    }
}

fn number_to_text_index_version(version: u32) -> TextIndexVersion {
    match version {
        1 => TextIndexVersion::V1,
        2 => TextIndexVersion::V2,
        3 => TextIndexVersion::V3,
        v => TextIndexVersion::Custom(v),
    }
}

fn object_id_to_value(o: &ObjectId) -> Value {
    to_document(o)
        .ok()
        .map_or(json!(null), |d| Value::from(document_to_json_map(&d)))
}

fn object_reference(obj: &MongoCollection) -> ObjectReference {
    ObjectReference {
        api_version: Some("pincette.net/v1".to_string()),
        field_path: None,
        kind: Some("MongoCollection".to_string()),
        name: obj.metadata.name.clone(),
        namespace: obj.metadata.namespace.clone(),
        resource_version: obj.resource_version(),
        uid: obj.uid(),
    }
}

fn options_to_model(options: &Options) -> IndexOptions {
    IndexOptions::builder()
        .bits(options.bits)
        .collation(options.collation.as_ref().map(collation_to_model))
        .default_language(options.default_language.clone())
        .expire_after(options.expire_after_seconds.map(Duration::from_secs))
        .hidden(options.hidden)
        .language_override(options.language_override.clone())
        .max(options.max)
        .min(options.min)
        .name(options.name.clone())
        .partial_filter_expression(
            options
                .partial_filter_expression
                .as_ref()
                .map(|m| map_to_document(&m, value_to_bson, all_entries)),
        )
        .sparse(options.sparse)
        .sphere_2d_index_version(
            options
                .sphere_index_version
                .map(number_to_sphere_index_version),
        )
        .text_index_version(options.text_index_version.map(number_to_text_index_version))
        .unique(options.unique)
        .weights(
            options
                .weights
                .as_ref()
                .map(|m| map_to_document(&m, |v| Bson::from(v), all_entries)),
        )
        .wildcard_projection(
            options
                .wildcard_projection
                .as_ref()
                .map(|m| map_to_document(&m, wildcard_projection_to_bson, all_entries)),
        )
        .build()
}

async fn patch_status(
    obj: &MongoCollection,
    client: &Client,
    error: Option<&OperatorError>,
) -> Result<MongoCollection, OperatorError> {
    let api = Api::<MongoCollection>::namespaced(client.clone(), name(&obj.metadata.namespace));
    let status = json!({"status": error.map_or(set_ready(obj.status.as_ref()),
        |e| set_error(obj.status.as_ref(), &e.to_string()))});

    api.patch_status(
        &obj.name_any(),
        &PatchParams {
            dry_run: false,
            force: false,
            field_manager: Some(CONTROLLER.to_string()),
            field_validation: None,
        },
        &Patch::Merge(&status),
    )
    .await
    .map_err(|e| OperatorError::StatusPatch(source_message(&e)))
}

async fn reconcile(obj: Arc<MongoCollection>, ctx: Arc<Data>) -> Result<Action, OperatorError> {
    if is_not_ready(&obj) {
        sleep(BACK_OFF).await;
    }

    let result = reconcile_action(&obj, &ctx).await;

    match result {
        Err(e) => {
            patch_status(&obj, &ctx.client, Some(&e)).await?;
            ctx.recorder
                .publish(&event(&e), &object_reference(&obj))
                .await?;
            Err(e)
        }
        Ok(r) => Ok(r),
    }
}

async fn reconcile_action(obj: &MongoCollection, ctx: &Data) -> Result<Action, OperatorError> {
    let invalid = invalid_keys(obj.spec.indexes.as_deref());

    if !invalid.is_empty() {
        Err(OperatorError::InvalidKeys(invalid.join(", ")))
    } else {
        let name = collection_name(obj);

        if !exists(&ctx.database, name).await? {
            create_collection(name, obj, &ctx.database).await?
        };

        let collection = ctx.database.collection(name);

        if reconcile_indexes(&collection, obj.spec.indexes.as_ref()).await?
            || obj.status.is_none()
            || is_not_ready(obj)
        // Leftover from previous attempt
        {
            patch_status(obj, &ctx.client, None).await?;
        }

        Ok(Action::requeue(INTERVAL))
    }
}

async fn reconcile_indexes(
    collection: &Collection<Document>,
    indexes: Option<&Vec<Index>>,
) -> Result<bool, OperatorError> {
    let found = list_indexes(collection).await?;
    let mut has_any = false;

    if let Some(i) = indexes {
        has_any |= drop_not_specified(collection, i.as_slice(), found.as_slice()).await?;
        has_any |= create_new_indexes(collection, i.as_slice(), found.as_slice()).await?;
    }

    Ok(has_any)
}

fn set_validator<'a>(c: CreateCollection<'a>, v: &Map<String, Value>) -> CreateCollection<'a> {
    match to_document(v) {
        Ok(v) => c.validator(v),
        Err(_) => c,
    }
}

fn sphere_index_version_to_number(version: Sphere2DIndexVersion) -> u32 {
    match version {
        Sphere2DIndexVersion::V2 => 2,
        Sphere2DIndexVersion::V3 => 3,
        Sphere2DIndexVersion::Custom(v) => v,
    }
}

fn source_message(error: &dyn Error) -> String {
    error.source().map_or(error.to_string(), |s| s.to_string())
}

fn text_index_keys(options: &Options) -> Option<Vec<Key>> {
    options.weights.as_ref().map(|w| {
        w.clone()
            .into_keys()
            .map(|k| Key {
                direction: None,
                index_type: Some(Text),
                field: k,
            })
            .collect()
    })
}

fn text_index_version_to_number(version: TextIndexVersion) -> u32 {
    match version {
        TextIndexVersion::V1 => 1,
        TextIndexVersion::V2 => 2,
        TextIndexVersion::V3 => 3,
        TextIndexVersion::Custom(v) => v,
    }
}

fn time_series(t: &TimeSeries) -> options::TimeseriesOptions {
    options::TimeseriesOptions::builder()
        .bucket_max_span(t.bucket_max_span_seconds.map(Duration::from_secs))
        .bucket_rounding(t.bucket_rounding_seconds.map(Duration::from_secs))
        .granularity(t.granularity.clone().map(time_series_granularity))
        .meta_field(t.meta_field.clone())
        .time_field(t.time_field.clone())
        .build()
}

fn time_series_granularity(g: Granularity) -> TimeseriesGranularity {
    match g {
        Granularity::Hours => TimeseriesGranularity::Hours,
        Granularity::Minutes => TimeseriesGranularity::Minutes,
        Granularity::Seconds => TimeseriesGranularity::Seconds,
    }
}

fn validation_action(a: ValidationAction) -> options::ValidationAction {
    match a {
        ValidationAction::Error => options::ValidationAction::Error,
        ValidationAction::Warn => options::ValidationAction::Warn,
    }
}

fn validation_level(l: ValidationLevel) -> options::ValidationLevel {
    match l {
        ValidationLevel::Moderate => options::ValidationLevel::Moderate,
        ValidationLevel::Off => options::ValidationLevel::Off,
        ValidationLevel::Strict => options::ValidationLevel::Strict,
    }
}

fn value_to_bson(v: &Value) -> Bson {
    Bson::try_from(v.clone()).ok().unwrap_or(Bson::Null)
}

fn value_to_wildcard_projection(v: u32) -> WildcardProjection {
    if v == 1 {
        WildcardProjection::Include
    } else {
        WildcardProjection::Exclude
    }
}

pub fn watch(client: Client) -> Vec<Api<MongoCollection>> {
    let namespaces = watch_namespaces();

    if namespaces.is_empty() || (namespaces.len() == 1 && namespaces[0] == "*") {
        info!("Watching at cluster scope");
        Vec::from([Api::<MongoCollection>::all(client)])
    } else {
        namespaces
            .iter()
            .map(|n| Api::<MongoCollection>::namespaced(client.clone(), n))
            .collect()
    }
}

fn wildcard_projection_to_bson(w: &WildcardProjection) -> Bson {
    match w {
        WildcardProjection::Exclude => Bson::from(0),
        WildcardProjection::Include => Bson::from(1),
    }
}
