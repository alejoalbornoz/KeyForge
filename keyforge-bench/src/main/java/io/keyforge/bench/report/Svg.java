package io.keyforge.bench.report;

import java.util.Locale;

/**
 * A small SVG writer shared by the charts: the document frame with its own opaque surface and light/dark theme, and
 * escaped text. A chart is shown as an image (GitHub's README), so it carries its colors itself: series colors are the
 * classes {@code f1..f8} (fill) and {@code l1..l8} (stroke), switched to their dark steps by
 * {@code prefers-color-scheme}.
 */
final class Svg {

    /** Categorical series colors, light and dark steps, in their fixed order. */
    private static final String[] LIGHT = {
        "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"};
    private static final String[] DARK = {
        "#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"};

    static final int SERIES_SLOTS = LIGHT.length;
    static final String FONT = "system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";

    private final StringBuilder out = new StringBuilder();

    Svg(int width, int height, String title) {
        out.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"").append(width).append("\" height=\"")
                .append(height).append("\" viewBox=\"0 0 ").append(width).append(' ').append(height)
                .append("\" role=\"img\" aria-label=\"").append(escape(title)).append("\" font-family=\"")
                .append(FONT).append("\">\n");
        out.append("<title>").append(escape(title)).append("</title>\n");
        out.append("<style>\n").append(css()).append("</style>\n");
        out.append("<rect class=\"surface\" x=\"0\" y=\"0\" width=\"").append(width).append("\" height=\"")
                .append(height).append("\" rx=\"8\"/>\n");
    }

    private static String css() {
        StringBuilder css = new StringBuilder();
        theme(css, "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e0", "#a3a29c", LIGHT);
        css.append("@media (prefers-color-scheme: dark) {\n");
        theme(css, "#1a1a19", "#f0efec", "#c3c2b7", "#383835", "#6b6a64", DARK);
        css.append("}\n");
        return css.toString();
    }

    private static void theme(StringBuilder css, String surface, String text, String muted, String grid, String axis,
                              String[] series) {
        css.append(".surface{fill:").append(surface).append("}")
                .append(".text{fill:").append(text).append("}")
                .append(".muted{fill:").append(muted).append("}")
                .append(".grid{stroke:").append(grid).append("}")
                .append(".axis{stroke:").append(axis).append("}")
                .append(".ring{stroke:").append(surface).append("}");
        for (int i = 0; i < series.length; i++) {
            css.append(".f").append(i + 1).append("{fill:").append(series[i]).append("}")
                    .append(".l").append(i + 1).append("{stroke:").append(series[i]).append("}");
        }
        css.append('\n');
    }

    /** Appends raw SVG markup. */
    Svg raw(String markup) {
        out.append(markup).append('\n');
        return this;
    }

    /** Text at (x, y); {@code anchor} is {@code start}, {@code middle} or {@code end}. */
    Svg text(double x, double y, String text, String cssClass, int size, String anchor, boolean bold) {
        out.append("<text x=\"").append(n(x)).append("\" y=\"").append(n(y)).append("\" class=\"").append(cssClass)
                .append("\" font-size=\"").append(size).append("\" text-anchor=\"").append(anchor).append('"');
        if (bold) {
            out.append(" font-weight=\"600\"");
        }
        out.append('>').append(escape(text)).append("</text>\n");
        return this;
    }

    Svg line(double x1, double y1, double x2, double y2, String cssClass, double strokeWidth) {
        out.append("<line x1=\"").append(n(x1)).append("\" y1=\"").append(n(y1)).append("\" x2=\"").append(n(x2))
                .append("\" y2=\"").append(n(y2)).append("\" class=\"").append(cssClass).append("\" stroke-width=\"")
                .append(n(strokeWidth)).append("\"/>\n");
        return this;
    }

    /** A legend entry: a swatch in series {@code slot} and its name. Returns the x after the entry. */
    double legendEntry(double x, double y, int slot, String name) {
        out.append("<rect x=\"").append(n(x)).append("\" y=\"").append(n(y - 9)).append("\" width=\"12\" height=\"12\" "
                + "rx=\"3\" class=\"f").append(slot).append("\"/>\n");
        text(x + 17, y + 1, name, "text", 12, "start", false);
        return x + 17 + approximateWidth(name, 12) + 18;
    }

    String finish() {
        return out + "</svg>\n";
    }

    /** Series class number (1-based) for the {@code index}-th series; there are {@link #SERIES_SLOTS} of them. */
    static int slot(int index) {
        if (index < 0 || index >= SERIES_SLOTS) {
            throw new IllegalArgumentException("Only " + SERIES_SLOTS + " series colors; fold the rest or facet");
        }
        return index + 1;
    }

    /** A rough text width, enough to lay out legends and margins without font metrics. */
    static double approximateWidth(String text, int size) {
        return text.length() * size * 0.56;
    }

    static String escape(String text) {
        StringBuilder escaped = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '<' -> escaped.append("&lt;");
                case '>' -> escaped.append("&gt;");
                case '&' -> escaped.append("&amp;");
                case '"' -> escaped.append("&quot;");
                default -> escaped.append(c);
            }
        }
        return escaped.toString();
    }

    /** A coordinate with at most one decimal. */
    static String n(double value) {
        String text = String.format(Locale.ROOT, "%.1f", value);
        return text.endsWith(".0") ? text.substring(0, text.length() - 2) : text;
    }

    /** 1234 -> "1.2k", 1500000 -> "1.5M", 950 -> "950". */
    static String compact(double value) {
        double magnitude = Math.abs(value);
        if (magnitude >= 1e6) {
            return trim(String.format(Locale.ROOT, magnitude >= 1e7 ? "%.0f" : "%.2f", value / 1e6)) + "M";
        }
        if (magnitude >= 1e3) {
            return trim(String.format(Locale.ROOT, magnitude >= 1e4 ? "%.0f" : "%.1f", value / 1e3)) + "k";
        }
        return trim(String.format(Locale.ROOT, "%.0f", value));
    }

    private static String trim(String number) {
        if (!number.contains(".")) {
            return number;
        }
        String trimmed = number.replaceAll("0+$", "");
        return trimmed.endsWith(".") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }
}
