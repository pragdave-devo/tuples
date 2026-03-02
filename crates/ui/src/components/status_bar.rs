use dioxus::prelude::*;
use crate::grpc;

#[component]
pub fn StatusBar() -> Element {
    let version = use_resource(|| async { grpc::get_version().await });

    match &*version.read_unchecked() {
        None => rsx! { span { "Connecting..." } },
        Some(Ok(v)) => rsx! { span { style: "color:green", "Server version: {v}" } },
        Some(Err(e)) => rsx! { span { style: "color:red", "Error: {e}" } },
    }
}
