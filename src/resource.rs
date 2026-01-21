use k8s_openapi::serde::{Deserialize, Serialize};
use kube::CustomResource;
use kube_operator_util::status::Status;
use schemars::{JsonSchema, JsonSchema_repr};
use serde_json::{Map, Value};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::cmp::PartialEq;
use std::collections::BTreeMap;
use CollationAlternate::NonIgnorable;
use CollationCaseFirst::Off;
use CollationMaxVariable::Punct;
use CollationStrength::Tertiary;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    kind = "MongoCollection",
    group = "pincette.net",
    version = "v1",
    namespaced,
    category = "controllers",
    shortname = "mc",
    printcolumn = r#"{"name":"Health", "type":"string", "jsonPath":".status.health.status"}"#,
    printcolumn = r#"{"name":"Phase", "type":"string", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
#[kube(status = "Status")]
#[serde(rename_all = "camelCase")]
pub struct MongoCollectionSpec {
    pub capped: Option<bool>,
    pub change_stream_pre_and_post_images: Option<bool>,
    pub clustered: Option<bool>,
    pub collation: Option<Collation>,
    pub expire_after_seconds: Option<u64>,
    pub indexes: Option<Vec<Index>>,
    pub max: Option<u64>,
    pub name: Option<String>,
    pub size: Option<u64>,
    pub time_series: Option<TimeSeries>,
    pub validator: Option<Map<String, Value>>,
    pub validation_action: Option<ValidationAction>,
    pub validation_level: Option<ValidationLevel>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Collation {
    #[serde(default = "Collation::default_alternate")]
    pub alternate: CollationAlternate,
    #[serde(default = "Collation::default_backwards")]
    pub backwards: bool,
    #[serde(default = "Collation::default_case_first")]
    pub case_first: CollationCaseFirst,
    #[serde(default = "Collation::default_case_level")]
    pub case_level: bool,
    pub locale: String,
    #[serde(default = "Collation::default_max_variable")]
    pub max_variable: CollationMaxVariable,
    #[serde(default = "Collation::default_normalization")]
    pub normalization: bool,
    #[serde(default = "Collation::default_numeric_ordering")]
    pub numeric_ordering: bool,
    #[serde(default = "Collation::default_strength")]
    pub strength: CollationStrength,
}

impl Collation {
    pub fn default_alternate() -> CollationAlternate {
        NonIgnorable
    }

    pub fn default_backwards() -> bool {
        false
    }

    pub fn default_case_first() -> CollationCaseFirst {
        Off
    }

    pub fn default_case_level() -> bool {
        false
    }

    pub fn default_max_variable() -> CollationMaxVariable {
        Punct
    }

    pub fn default_normalization() -> bool {
        false
    }

    pub fn default_numeric_ordering() -> bool {
        false
    }

    pub fn default_strength() -> CollationStrength {
        Tertiary
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CollationAlternate {
    NonIgnorable,
    Shifted,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CollationCaseFirst {
    Upper,
    Lower,
    Off,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CollationMaxVariable {
    Punct,
    Space,
}

#[derive(Clone, Debug, Deserialize_repr, Serialize_repr, JsonSchema_repr, PartialEq)]
#[repr(i32)]
pub enum CollationStrength {
    Primary = 1,
    Secondary = 2,
    Tertiary = 3,
    Quaternary = 4,
    Identical = 5,
}

#[derive(Clone, Debug, Deserialize_repr, Serialize_repr, JsonSchema_repr, PartialEq)]
#[repr(i32)]
pub enum Direction {
    Ascending = 1,
    Descending = -1,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Granularity {
    Hours,
    Minutes,
    Seconds,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Index {
    pub keys: Vec<Key>,
    pub options: Option<Options>,
}

impl PartialEq for Index {
    fn eq(&self, other: &Self) -> bool {
        same_keys(self.keys.as_slice(), other.keys.as_slice())
            && (self.options == other.options || is_default_option(&self.options, &other.options))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum IndexType {
    Hashed,
    Text,
    #[serde(rename = "2d")]
    TwoDimensional,
    #[serde(rename = "2dsphere")]
    TwoDimensionalSphere,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Key {
    pub direction: Option<Direction>,
    pub field: String,
    pub index_type: Option<IndexType>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Options {
    pub bits: Option<u32>,
    pub collation: Option<Collation>,
    pub default_language: Option<String>,
    pub expire_after_seconds: Option<u64>,
    pub hidden: Option<bool>,
    pub language_override: Option<String>,
    pub max: Option<f64>,
    pub min: Option<f64>,
    pub name: Option<String>,
    pub partial_filter_expression: Option<BTreeMap<String, Value>>,
    pub sparse: Option<bool>,
    pub sphere_index_version: Option<u32>,
    pub text_index_version: Option<u32>,
    pub unique: Option<bool>,
    pub weights: Option<BTreeMap<String, u32>>,
    pub wildcard_projection: Option<BTreeMap<String, WildcardProjection>>,
}

impl Options {
    fn is_default(&self) -> bool {
        self.bits.is_none_or(|v| v == 26)
            && self.collation.is_none()
            && self.default_language.as_ref().is_none_or(|v| v == "english")
            && self.expire_after_seconds.is_none()
            && self.hidden.is_none_or(|v| !v)
            && self.language_override.as_ref().is_none_or(|v| v == "language")
            && self.max.is_none_or(|v| v == 180.0)
            && self.min.is_none_or(|v| v == -180.0)
            && self.partial_filter_expression.is_none()
            && self.sparse.is_none_or(|v| !v)
            && self.sphere_index_version.is_none()
            && self.text_index_version.is_none()
            && self.unique.is_none_or(|v| !v)
            && self.weights.is_none()
            && self.wildcard_projection.is_none()
    }
}

// The name is excluded because it may be a generated name.
impl PartialEq for Options {
    fn eq(&self, other: &Self) -> bool {
        self.bits == other.bits
            && self.collation == other.collation
            && (self.default_language == other.default_language
                || is_default_language(&self.default_language, &other.default_language))
            && self.expire_after_seconds == other.expire_after_seconds
            && self.hidden == other.hidden
            && (self.language_override == other.language_override
                || is_default_language_override(&self.language_override, &other.language_override))
            && self.max == other.max
            && self.min == other.min
            && self.partial_filter_expression == other.partial_filter_expression
            && (self.sphere_index_version == other.sphere_index_version
                || self.sphere_index_version.is_none()
                || other.sphere_index_version.is_none())
            && (self.text_index_version == other.text_index_version
                || self.text_index_version.is_none()
                || other.text_index_version.is_none())
            && self.unique == other.unique
            && (self.weights == other.weights || self.weights.is_none() || other.weights.is_none())
            && self.wildcard_projection == other.wildcard_projection
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimeSeries {
    pub bucket_max_span_seconds: Option<u64>,
    pub bucket_rounding_seconds: Option<u64>,
    pub granularity: Option<Granularity>,
    pub meta_field: Option<String>,
    pub time_field: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ValidationAction {
    Error,
    Warn,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ValidationLevel {
    Moderate,
    Off,
    Strict,
}

#[derive(Clone, Debug, Deserialize_repr, Serialize_repr, JsonSchema_repr, PartialEq)]
#[repr(i32)]
pub enum WildcardProjection {
    Exclude = 0,
    Include = 1,
}

fn is_default_comparison<T, F>(v1: Option<&T>, v2: Option<&T>, is_default: F) -> bool
where
    F: Fn(&T) -> bool,
{
    (v1.is_none() && v2.is_some_and(&is_default))
        || (v2.is_none() && v1.is_some_and(&is_default))
        || (v1.is_some_and(&is_default) && v2.is_some_and(&is_default))
}

fn is_default_language(v1: &Option<String>, v2: &Option<String>) -> bool {
    is_default_comparison(v1.as_ref(), v2.as_ref(), |v| v == "english")
}

fn is_default_language_override(v1: &Option<String>, v2: &Option<String>) -> bool {
    is_default_comparison(v1.as_ref(), v2.as_ref(), |v| v == "language")
}

fn is_default_option(v1: &Option<Options>, v2: &Option<Options>) -> bool {
    is_default_comparison(v1.as_ref(), v2.as_ref(), |v| v.is_default())
}

fn same_keys(v1: &[Key], v2: &[Key]) -> bool {
    v1.len() == v2.len() && v1.iter().all(|k| v2.contains(k))
}
