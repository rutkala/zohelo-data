(() => {
  "use strict";

  const READY = "zohelo-catalogue-viewer-ready";
  const REQUEST = "zohelo-catalogue-viewer-request";
  const DOCUMENT = "zohelo-catalogue-viewer-document";
  const ERROR = "zohelo-catalogue-viewer-error";
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
    const scriptUrl = URL.createObjectURL(
      new Blob([script.textContent], { type: "text/javascript" })
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
