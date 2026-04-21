from __future__ import annotations

from fastapi.responses import HTMLResponse


SITE_TITLE = "West Coast KBP"


def _layout(*, title: str, active_path: str, body: str) -> HTMLResponse:
    home_class = "site-nav__link is-active" if active_path == "/" else "site-nav__link"
    contact_class = "site-nav__link is-active" if active_path == "/contact" else "site-nav__link"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title}</title>
    <link rel="stylesheet" href="/static/site.css">
</head>
<body>
    <div class="site-shell">
        <header class="site-header">
            <div class="site-header__inner">
                <a class="site-brand" href="/">{SITE_TITLE}</a>
                <nav class="site-nav" aria-label="Primary">
                    <a class="{home_class}" href="/">Home</a>
                    <a class="{contact_class}" href="/contact">Contact</a>
                </nav>
            </div>
        </header>
        <main class="site-main">
            {body}
        </main>
        <footer class="site-footer">
            <div class="site-footer__inner">
                <p class="site-footer__text">{SITE_TITLE}</p>
                <a class="site-footer__link" href="/contact">Contact Us</a>
            </div>
        </footer>
    </div>
</body>
</html>"""
    return HTMLResponse(content=html)


def home_page() -> HTMLResponse:
    body = """
    <section class="hero">
        <div class="section-shell">
            <p class="section-kicker">Company Presentation</p>
            <h1 class="section-title">West Coast KBP project support, clearly presented.</h1>
            <p class="section-copy">
                This is the approved first-pass scaffold for the company website. It establishes
                the layout, routing, and section structure without adding unapproved features.
            </p>
            <div class="cta-row">
                <a class="button button--primary" href="/contact">Contact</a>
                <a class="button button--secondary" href="/contact">Request a Quote</a>
            </div>
        </div>
    </section>
    <section class="content-section">
        <div class="section-shell">
            <p class="section-kicker">Company Overview</p>
            <h2 class="section-title">Who we are</h2>
            <p class="section-copy">
                Placeholder company overview content block aligned to the approved scope.
            </p>
        </div>
    </section>
    <section class="content-section">
        <div class="section-shell">
            <p class="section-kicker">Services Overview</p>
            <h2 class="section-title">What we do</h2>
            <div class="card-grid">
                <article class="info-card"><h3>Service Category One</h3><p>Placeholder service description.</p></article>
                <article class="info-card"><h3>Service Category Two</h3><p>Placeholder service description.</p></article>
                <article class="info-card"><h3>Service Category Three</h3><p>Placeholder service description.</p></article>
            </div>
        </div>
    </section>
    <section class="content-section">
        <div class="section-shell">
            <p class="section-kicker">Why Choose Us</p>
            <h2 class="section-title">Trust-building section</h2>
            <ul class="bullet-list">
                <li>Responsive communication</li>
                <li>Reliable field execution</li>
                <li>Clear next steps</li>
            </ul>
        </div>
    </section>
    <section class="content-section content-section--accent">
        <div class="section-shell">
            <p class="section-kicker">Lead Capture</p>
            <h2 class="section-title">Ready to discuss your project?</h2>
            <p class="section-copy">
                The lead capture path is intentionally represented as a scaffold CTA in T2.
            </p>
            <a class="button button--primary" href="/contact">Open Contact Page</a>
        </div>
    </section>
    """
    return _layout(title=f"{SITE_TITLE} | Home", active_path="/", body=body)


def contact_page() -> HTMLResponse:
    body = """
    <section class="hero hero--compact">
        <div class="section-shell">
            <p class="section-kicker">Contact</p>
            <h1 class="section-title">Contact West Coast KBP</h1>
            <p class="section-copy">
                This page provides direct contact access and the approved lead capture skeleton.
            </p>
        </div>
    </section>
    <section class="content-section">
        <div class="section-shell two-column">
            <div class="stack">
                <h2 class="section-title">Contact Methods</h2>
                <p class="section-copy">Phone: placeholder</p>
                <p class="section-copy">Email: placeholder</p>
                <p class="section-copy">Service Area: placeholder</p>
            </div>
            <div class="stack">
                <h2 class="section-title">Lead Capture Form</h2>
                <form class="lead-form">
                    <label class="field">
                        <span>Name</span>
                        <input type="text" name="name" />
                    </label>
                    <label class="field">
                        <span>Email or Phone</span>
                        <input type="text" name="contact" />
                    </label>
                    <label class="field">
                        <span>Service Needed</span>
                        <input type="text" name="service_needed" />
                    </label>
                    <label class="field">
                        <span>Message</span>
                        <textarea name="message" rows="5"></textarea>
                    </label>
                    <button class="button button--primary" type="button">Submit Inquiry</button>
                </form>
            </div>
        </div>
    </section>
    <section class="content-section">
        <div class="section-shell">
            <h2 class="section-title">Confirmation State</h2>
            <p class="section-copy">
                Confirmation behavior is reserved for the next implementation task. This placeholder
                block marks the approved location of that state in the page flow.
            </p>
        </div>
    </section>
    """
    return _layout(title=f"{SITE_TITLE} | Contact", active_path="/contact", body=body)
