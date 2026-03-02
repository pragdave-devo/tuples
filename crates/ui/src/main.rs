#![allow(non_snake_case)]

mod components;
mod grpc;
mod theme;

use components::nav::{NavBar, Page};
use components::schemas::SchemasPage;
use components::StatusBar;
use dioxus::prelude::*;
use dioxus_liveview::LiveviewRouter;

fn App() -> Element {
    let page = use_signal(|| Page::Schemas);
    let mut dark = use_signal(|| false);

    rsx! {
        style { {theme::THEME_CSS} }
        div {
            "data-theme": if *dark.read() { "dark" } else { "light" },
            style: "min-height:100vh; background:var(--bg); \
                    font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;",

          div {
            style: "max-width:900px; margin:0 auto; padding:1.5rem;",

            // title bar
            div {
                style: "display:flex; align-items:center; justify-content:space-between; \
                        margin-bottom:0.75rem;",
                h1 { style: "margin:0; font-size:1.4rem; color:var(--text-primary); font-weight:700;",
                    "Pattern Match"
                }
                div { style: "display:flex; align-items:center; gap:0.75rem;",
                    StatusBar {}
                    button {
                        style: "padding:0.3rem 0.6rem; border:1px solid var(--border); \
                                border-radius:6px; background:var(--bg-surface); \
                                cursor:pointer; font-size:1rem; line-height:1;",
                        onclick: move |_| { let cur = *dark.read(); dark.set(!cur); },
                        if *dark.read() { "\u{2600}" } else { "\u{263D}" }
                    }
                }
            }

            NavBar { current: page }

            div { style: "padding-top:1.5rem;",
                match *page.read() {
                    Page::Schemas => rsx! { SchemasPage {} },
                    _ => rsx! {
                        div { style: "text-align:center; padding:4rem; color:var(--text-muted);",
                            "Coming soon"
                        }
                    },
                }
            }
          }
        }
    }
}

#[tokio::main]
async fn main() {
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], 3030).into();

    // `with_app` registers "/*route" which doesn't match bare "/" in axum 0.7,
    // so we add a fallback that serves the same index page.
    let app = axum::Router::new()
        .with_app("/", App)
        .fallback(axum::routing::get(|| async {
            axum::response::Html(format!(
                r#"<!DOCTYPE html>
<html>
    <head><title>Pattern Match</title></head>
    <body><div id="main"></div></body>
    {}
</html>"#,
                dioxus_liveview::interpreter_glue("/ws"),
            ))
        }));

    println!("Listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}
