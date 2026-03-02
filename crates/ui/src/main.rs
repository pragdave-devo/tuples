mod components;
mod grpc;

use components::nav::{NavBar, Page};
use components::schemas::SchemasPage;
use components::StatusBar;
use dioxus::prelude::*;

fn main() {
    dioxus::launch(App);
}

fn App() -> Element {
    let page = use_signal(|| Page::Schemas);

    rsx! {
        div {
            style: "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; \
                    max-width:900px; margin:0 auto; padding:1.5rem;",

            // title bar
            div {
                style: "display:flex; align-items:center; justify-content:space-between; \
                        margin-bottom:0.75rem;",
                h1 { style: "margin:0; font-size:1.4rem; color:#0f172a; font-weight:700;",
                    "Pattern Match"
                }
                StatusBar {}
            }

            NavBar { current: page }

            div { style: "padding-top:1.5rem;",
                match *page.read() {
                    Page::Schemas => rsx! { SchemasPage {} },
                    _ => rsx! {
                        div { style: "text-align:center; padding:4rem; color:#94a3b8;",
                            "Coming soon"
                        }
                    },
                }
            }
        }
    }
}
