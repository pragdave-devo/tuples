use dioxus::prelude::*;

#[derive(Clone, Copy, PartialEq)]
pub enum Page {
    History,
    Playbooks,
    Agents,
    Schemas,
}

#[component]
pub fn NavBar(current: Signal<Page>) -> Element {
    rsx! {
        nav {
            style: "display:flex; border-bottom:2px solid #e2e8f0; margin-bottom:0;",
            NavTab { label: "History",   page: Page::History,   current }
            NavTab { label: "Playbooks", page: Page::Playbooks, current }
            NavTab { label: "Agents",    page: Page::Agents,    current }
            NavTab { label: "Schemas",   page: Page::Schemas,   current }
        }
    }
}

#[component]
fn NavTab(label: &'static str, page: Page, mut current: Signal<Page>) -> Element {
    let active = *current.read() == page;
    rsx! {
        button {
            style: if active {
                "padding:0.6rem 1.25rem; border:none; border-bottom:3px solid #2563eb; \
                 background:none; color:#2563eb; cursor:pointer; font-size:0.9rem; \
                 font-weight:600; margin-bottom:-2px;"
            } else {
                "padding:0.6rem 1.25rem; border:none; border-bottom:3px solid transparent; \
                 background:none; color:#64748b; cursor:pointer; font-size:0.9rem; \
                 margin-bottom:-2px;"
            },
            onclick: move |_| current.set(page),
            "{label}"
        }
    }
}
