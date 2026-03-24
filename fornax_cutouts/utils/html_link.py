def html_link(url: str, text: str) -> str:
    """Return an HTML anchor that opens in a new tab, for OpenAPI descriptions and other HTML output."""
    return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{text}</a>'
