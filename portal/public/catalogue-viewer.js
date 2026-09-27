(() => {
  "use strict";

  const READY = "zohelo-catalogue-viewer-ready";
  const REQUEST = "zohelo-catalogue-viewer-request";
  const DOCUMENT = "zohelo-catalogue-viewer-document";
  const ERROR = "zohelo-catalogue-viewer-error";
  const responsiveStyles = `
.zohelo-catalogue-nav-toggle,
.zohelo-catalogue-nav-close,
.zohelo-catalogue-nav-backdrop {
  display: none;
}

@media (max-width: 767px) {
  html,
  body,
  .layout,
  .app {
    width: 100% !important;
    max-width: 100% !important;
    min-width: 0 !important;
    overflow-x: hidden !important;
  }

  .app-content {
    width: 100% !important;
    min-width: 0 !important;
    flex: 1 1 100% !important;
    overflow: hidden;
  }

  .app-content > .app-body,
  .app-frame {
    width: 100%;
    min-width: 0;
    max-width: 100%;
  }

  .app-content .app-navbar .app-frame {
    padding-left: 12px;
    padding-right: 12px;
  }

  .app-content .app-navbar input {
    width: auto;
    min-width: 0;
    flex: 1 1 auto;
  }

  .app-content .app-scroll {
    overscroll-behavior-x: contain;
  }

  .app-content .app-pad {
    padding-left: 16px;
    padding-right: 16px;
  }

  .app-menu {
    position: fixed !important;
    z-index: 1002;
    inset: 0 auto 0 0;
    width: min(86vw, 340px) !important;
    min-width: 0 !important;
    max-width: 340px;
    transform: translateX(-105%);
    box-shadow: 8px 0 24px rgba(0, 16, 32, 0.2);
  }

  .zohelo-catalogue-nav-open .app-menu {
    transform: translateX(0);
  }

  .zohelo-catalogue-nav-toggle,
  .zohelo-catalogue-nav-close {
    min-width: 44px;
    min-height: 40px;
    border: 1px solid #c9d0d6;
    border-radius: 4px;
    background: #fff;
    color: #24292e;
    font: inherit;
    cursor: pointer;
  }

  .zohelo-catalogue-nav-toggle {
    display: inline-flex;
    flex: 0 0 auto;
    align-items: center;
    justify-content: center;
    gap: 6px;
    margin-right: 10px;
    padding: 7px 10px;
    font-weight: 600;
  }

  .zohelo-catalogue-nav-close {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    margin-left: 8px;
    padding: 4px 12px;
    font-size: 24px;
    line-height: 1;
  }

  .zohelo-catalogue-nav-toggle:focus-visible,
  .zohelo-catalogue-nav-close:focus-visible {
    outline: 3px solid #00aaaa;
    outline-offset: 2px;
  }

  .zohelo-catalogue-nav-backdrop {
    position: fixed;
    z-index: 1001;
    inset: 0;
    border: 0;
    background: rgba(0, 16, 32, 0.42);
  }

  .zohelo-catalogue-nav-open .zohelo-catalogue-nav-backdrop {
    display: block;
  }

  .table-responsive,
  pre {
    max-width: 100%;
    overflow-x: auto;
  }
}`;

  // Runs after dbt's pinned bundle inside the generated document.
  function installResponsiveNavigation() {
    const mobile = matchMedia("(max-width: 767px)");
    const root = document.documentElement;
    let menu;
    let content;
    let toggle;
    let close;
    let backdrop;

    const setOpen = (open, restoreFocus = true) => {
      if (!menu || !toggle) return;
      const mobileOpen = mobile.matches && open;
      root.classList.toggle("zohelo-catalogue-nav-open", mobileOpen);
      toggle.setAttribute("aria-expanded", String(mobileOpen));
      menu.setAttribute("aria-hidden", String(mobile.matches && !mobileOpen));
      menu.inert = mobile.matches && !mobileOpen;
      if (content) content.inert = mobileOpen;
      if (mobileOpen) close?.focus();
      else if (restoreFocus) toggle.focus();
    };

    const enhanceSwitches = () => {
      menu?.querySelectorAll(".switch-label").forEach((item) => {
        if (item.dataset.zoheloKeyboardButton) return;
        item.dataset.zoheloKeyboardButton = "true";
        item.setAttribute("role", "button");
        item.setAttribute("tabindex", "0");
        item.addEventListener("keydown", (event) => {
          if (event.key !== "Enter" && event.key !== " ") return;
          event.preventDefault();
          item.click();
        });
      });
    };

    const enhance = () => {
      if (toggle) {
        enhanceSwitches();
        return true;
      }
      menu = document.querySelector(".app-menu");
      content = document.querySelector(".app-content");
      const toolbar = content?.querySelector(".app-navbar .app-frame");
      if (!menu || !content || !toolbar || !document.body) return false;

      menu.id ||= "zohelo-catalogue-navigation";
      toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "zohelo-catalogue-nav-toggle";
      toggle.setAttribute("aria-controls", menu.id);
      toggle.setAttribute("aria-expanded", "false");
      toggle.setAttribute("aria-label", "Browse catalogue");
      toggle.innerHTML = '<span aria-hidden="true">☰</span><span>Browse</span>';
      toggle.addEventListener("click", () => setOpen(true));
      toolbar.prepend(toggle);

      close = document.createElement("button");
      close.type = "button";
      close.className = "zohelo-catalogue-nav-close";
      close.setAttribute("aria-label", "Close catalogue navigation");
      close.textContent = "×";
      close.addEventListener("click", () => setOpen(false));
      menu.querySelector(".app-navbar .app-row")?.append(close);

      backdrop = document.createElement("button");
      backdrop.type = "button";
      backdrop.className = "zohelo-catalogue-nav-backdrop";
      backdrop.setAttribute("aria-label", "Close catalogue navigation");
      backdrop.addEventListener("click", () => setOpen(false));
      // dbt's fixed .app establishes a stacking context. Keep the backdrop
      // beside the menu so its lower z-index actually places it behind it.
      menu.parentElement.append(backdrop);

      menu.addEventListener("click", (event) => {
        if (
          event.target instanceof Element &&
          event.target.closest("a[data-nav-unique-id], a[ui-sref]")
        ) {
          setOpen(false, false);
        }
      });
      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && root.classList.contains("zohelo-catalogue-nav-open")) {
          setOpen(false);
        }
      });
      mobile.addEventListener("change", () => setOpen(false, false));
      enhanceSwitches();
      setOpen(false, false);
      return true;
    };

    const observer = new MutationObserver(() => {
      enhance();
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
    addEventListener("DOMContentLoaded", enhance, { once: true });
    enhance();
  }
  const nonce = globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random()}`;
  let received = false;
  let rendering = false;
  const announce = () => parent.postMessage({ type: READY, nonce }, "*");
  const report = (message) => parent.postMessage({ type: ERROR, nonce, message }, "*");
  const readyTimer = setInterval(announce, 250);

  const fail = (message) => {
    report(message);
    document.body.textContent = message;
    throw new Error(message);
  };

  const render = (html) => {
    const parsed = new DOMParser().parseFromString(html, "text/html");
    const scripts = Array.from(parsed.querySelectorAll("script"));
    if (scripts.length !== 1 || scripts[0].src || !scripts[0].textContent) {
      fail("The catalogue viewer received an incompatible dbt Docs document.");
    }
    const script = scripts[0];
    const style = parsed.createElement("style");
    style.dataset.zoheloCatalogueResponsive = "true";
    style.textContent = responsiveStyles;
    parsed.head.append(style);
    const scriptUrl = URL.createObjectURL(
      new Blob(
        [script.textContent, `\n;(${installResponsiveNavigation.toString()})();`],
        { type: "text/javascript" }
      )
    );
    script.textContent = "";
    script.src = scriptUrl;
    rendering = true;
    document.open();
    // document.open removes prior document AND window listeners.
    addEventListener(
      "error",
      () => {
        if (rendering) report("The catalogue viewer could not load its dbt Docs script.");
      },
      true
    );
    document.write("<!doctype html>" + parsed.documentElement.outerHTML);
    document.close();
  };

  addEventListener("message", (event) => {
    const message = event.data;
    if (event.source !== parent || !message || typeof message !== "object") return;
    if (message.type === REQUEST) {
      if (!received) announce();
      return;
    }
    if (
      received ||
      message.type !== DOCUMENT ||
      message.nonce !== nonce ||
      typeof message.html !== "string"
    ) {
      return;
    }
    received = true;
    clearInterval(readyTimer);
    render(message.html);
  });

  announce();
})();
