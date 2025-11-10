use kube::CustomResourceExt;
use resource::MongoCollection;

fn main() {
    print!(
        "{}",
        serde_json::to_string(&MongoCollection::crd()).unwrap()
    )
}
