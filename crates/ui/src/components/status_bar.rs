use dioxus::prelude::*;
use crate::grpc;

#[component]
pub fn StatusBar() -> Element {
    let version = use_resource(|| async { grpc::get_version().await });

    match &*version.read_unchecked() {
        None => rsx! { span { "Connecting..." } },
        Some(Ok(v)) => rsx! { span { style: "color:var(--success)", "Server version: {v}" } },
        Some(Err(e)) => rsx! { span { style: "color:var(--error)", "Error: {e}" } },
    }
}
