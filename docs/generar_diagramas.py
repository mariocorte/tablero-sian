import math
import os
from typing import Iterable, List, Sequence, Tuple

PAGE_WIDTH = 612  # 8.5 in * 72 dpi
PAGE_HEIGHT = 792  # 11 in * 72 dpi
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "diagramas_tablero_sian.pdf")


Number = float


def _fmt(value: Number) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".") if not float(value).is_integer() else str(int(value))


def _escape_text(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


class SimplePDF:
    def __init__(self) -> None:
        self._objects: List[dict] = []
        self._pages: List[Tuple[int, int]] = []
        self._font_id = self._add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    def _add_object(self, data: str) -> int:
        self._objects.append({"data": data})
        return len(self._objects)

    def add_page(self, commands: Iterable[str]) -> None:
        content = "\n".join(commands) + "\n"
        stream = f"<< /Length {len(content.encode('utf-8'))} >>\nstream\n{content}endstream"
        content_id = self._add_object(stream)
        page_template = (
            "<< /Type /Page /Parent __PAGES__ 0 R /MediaBox [0 0 {w} {h}] "
            "/Resources << /Font << /F1 {font} 0 R >> >> /Contents {content} 0 R >>"
        ).format(w=_fmt(PAGE_WIDTH), h=_fmt(PAGE_HEIGHT), font=self._font_id, content=content_id)
        page_id = self._add_object(page_template)
        self._pages.append((page_id, content_id))

    def save(self, path: str) -> None:
        page_ids = [page_id for page_id, _ in self._pages]
        pages_object_content = "<< /Type /Pages /Kids [" + " ".join(f"{pid} 0 R" for pid in page_ids) + f"] /Count {len(page_ids)} >>"
        pages_id = self._add_object(pages_object_content)

        # Replace placeholder with actual pages object reference
        for page_id, _ in self._pages:
            data = self._objects[page_id - 1]["data"].replace("__PAGES__", str(pages_id))
            self._objects[page_id - 1]["data"] = data

        catalog_id = self._add_object(f"<< /Type /Catalog /Pages {pages_id} 0 R >>")

        xref_positions: List[int] = []
        output = ["%PDF-1.4\n%\xFF\xFF\xFF\xFF\n"]
        for index, obj in enumerate(self._objects, start=1):
            xref_positions.append(sum(len(part.encode("latin1")) for part in output))
            output.append(f"{index} 0 obj\n{obj['data']}\nendobj\n")

        xref_start = sum(len(part.encode("latin1")) for part in output)
        xref = ["xref\n0 {count}\n".format(count=len(self._objects) + 1), "0000000000 65535 f \n"]
        for pos in xref_positions:
            xref.append(f"{pos:010} 00000 n \n")
        xref.append("trailer\n")
        xref.append(f"<< /Size {len(self._objects) + 1} /Root {catalog_id} 0 R >>\n")
        xref.append(f"startxref\n{xref_start}\n%%EOF")
        output.extend(xref)

        with open(path, "wb") as pdf_file:
            pdf_file.write("".join(output).encode("latin1"))


def _add_text(commands: List[str], text: str, x: Number, y: Number, size: Number, align: str = "left") -> None:
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if align == "center":
            estimated_width = len(line) * size * 0.45
            tx = x - estimated_width / 2
        elif align == "right":
            estimated_width = len(line) * size * 0.45
            tx = x - estimated_width
        else:
            tx = x
        ty = y - i * size * 1.2
        commands.extend(
            [
                "BT",
                f"/F1 {size} Tf",
                f"{_fmt(tx)} {_fmt(ty)} Td",
                f"({_escape_text(line)}) Tj",
                "ET",
            ]
        )


def _draw_rectangle(commands: List[str], x: Number, y: Number, width: Number, height: Number, fill: Tuple[Number, Number, Number], stroke: Tuple[Number, Number, Number]) -> None:
    commands.append(f"{_fmt(fill[0])} {_fmt(fill[1])} {_fmt(fill[2])} rg")
    commands.append(f"{_fmt(x)} {_fmt(y)} {_fmt(width)} {_fmt(height)} re f")
    commands.append(f"{_fmt(stroke[0])} {_fmt(stroke[1])} {_fmt(stroke[2])} RG")
    commands.append(f"{_fmt(x)} {_fmt(y)} {_fmt(width)} {_fmt(height)} re S")
    commands.append("0 0 0 rg")


def _draw_arrow(commands: List[str], start: Tuple[Number, Number], end: Tuple[Number, Number], arrow_size: Number = 8) -> None:
    commands.append("0 0 0 RG")
    commands.append(f"{_fmt(start[0])} {_fmt(start[1])} m")
    commands.append(f"{_fmt(end[0])} {_fmt(end[1])} l S")
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    length = math.hypot(dx, dy)
    if length == 0:
        return
    ux, uy = dx / length, dy / length
    left = (end[0] - arrow_size * ux - arrow_size * 0.6 * uy, end[1] - arrow_size * uy + arrow_size * 0.6 * ux)
    right = (end[0] - arrow_size * ux + arrow_size * 0.6 * uy, end[1] - arrow_size * uy - arrow_size * 0.6 * ux)
    commands.append("0 0 0 rg")
    commands.append(f"{_fmt(end[0])} {_fmt(end[1])} m")
    commands.append(f"{_fmt(left[0])} {_fmt(left[1])} l")
    commands.append(f"{_fmt(right[0])} {_fmt(right[1])} l f")


def _draw_polyline_with_arrow(commands: List[str], points: Sequence[Tuple[Number, Number]]) -> None:
    if len(points) < 2:
        return
    commands.append("0 0 0 RG")
    commands.append(f"{_fmt(points[0][0])} {_fmt(points[0][1])} m")
    for point in points[1:]:
        commands.append(f"{_fmt(point[0])} {_fmt(point[1])} l")
    commands.append("S")
    start = points[-2]
    end = points[-1]
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    length = math.hypot(dx, dy)
    if length == 0:
        return
    ux, uy = dx / length, dy / length
    left = (end[0] - 8 * ux - 8 * 0.6 * uy, end[1] - 8 * uy + 8 * 0.6 * ux)
    right = (end[0] - 8 * ux + 8 * 0.6 * uy, end[1] - 8 * uy - 8 * 0.6 * ux)
    commands.append("0 0 0 rg")
    commands.append(f"{_fmt(end[0])} {_fmt(end[1])} m")
    commands.append(f"{_fmt(left[0])} {_fmt(left[1])} l")
    commands.append(f"{_fmt(right[0])} {_fmt(right[1])} l f")


def _draw_ellipse(commands: List[str], center: Tuple[Number, Number], rx: Number, ry: Number, fill: Tuple[Number, Number, Number], stroke: Tuple[Number, Number, Number]) -> None:
    kappa = 0.5522847498
    cx, cy = center
    commands.append(f"{_fmt(fill[0])} {_fmt(fill[1])} {_fmt(fill[2])} rg")
    commands.append(f"{_fmt(cx + rx)} {_fmt(cy)} m")
    commands.append(
        f"{_fmt(cx + rx)} {_fmt(cy + ry * kappa)} {_fmt(cx + rx * kappa)} {_fmt(cy + ry)} {_fmt(cx)} {_fmt(cy + ry)} c"
    )
    commands.append(
        f"{_fmt(cx - rx * kappa)} {_fmt(cy + ry)} {_fmt(cx - rx)} {_fmt(cy + ry * kappa)} {_fmt(cx - rx)} {_fmt(cy)} c"
    )
    commands.append(
        f"{_fmt(cx - rx)} {_fmt(cy - ry * kappa)} {_fmt(cx - rx * kappa)} {_fmt(cy - ry)} {_fmt(cx)} {_fmt(cy - ry)} c"
    )
    commands.append(
        f"{_fmt(cx + rx * kappa)} {_fmt(cy - ry)} {_fmt(cx + rx)} {_fmt(cy - ry * kappa)} {_fmt(cx + rx)} {_fmt(cy)} c"
    )
    commands.append("f")
    commands.append(f"{_fmt(stroke[0])} {_fmt(stroke[1])} {_fmt(stroke[2])} RG")
    commands.append(f"{_fmt(cx + rx)} {_fmt(cy)} m")
    commands.append(
        f"{_fmt(cx + rx)} {_fmt(cy + ry * kappa)} {_fmt(cx + rx * kappa)} {_fmt(cy + ry)} {_fmt(cx)} {_fmt(cy + ry)} c"
    )
    commands.append(
        f"{_fmt(cx - rx * kappa)} {_fmt(cy + ry)} {_fmt(cx - rx)} {_fmt(cy + ry * kappa)} {_fmt(cx - rx)} {_fmt(cy)} c"
    )
    commands.append(
        f"{_fmt(cx - rx)} {_fmt(cy - ry * kappa)} {_fmt(cx - rx * kappa)} {_fmt(cy - ry)} {_fmt(cx)} {_fmt(cy - ry)} c"
    )
    commands.append(
        f"{_fmt(cx + rx * kappa)} {_fmt(cy - ry)} {_fmt(cx + rx)} {_fmt(cy - ry * kappa)} {_fmt(cx + rx)} {_fmt(cy)} c"
    )
    commands.append("S")
    commands.append("0 0 0 rg")


def _draw_actor(commands: List[str], base: Tuple[Number, Number], label: str, align: str = "left") -> None:
    x, y = base
    head_center = (x, y + 40)
    _draw_ellipse(commands, head_center, 12, 12, (1, 1, 1), (0, 0, 0))
    commands.append("0 0 0 RG")
    commands.append(f"{_fmt(x)} {_fmt(y + 28)} m {_fmt(x)} {_fmt(y)} l S")
    commands.append(f"{_fmt(x - 18)} {_fmt(y + 20)} m {_fmt(x + 18)} {_fmt(y + 20)} l S")
    commands.append(f"{_fmt(x)} {_fmt(y)} m {_fmt(x - 16)} {_fmt(y - 28)} l S")
    commands.append(f"{_fmt(x)} {_fmt(y)} m {_fmt(x + 16)} {_fmt(y - 28)} l S")
    text_align = "right" if align == "right" else "left"
    text_x = x - 20 if align == "right" else x + 20
    _add_text(commands, label, text_x, y + 60, 12, align=text_align)


def build_flowchart_page() -> List[str]:
    commands: List[str] = ["q", "1 w"]
    _add_text(commands, "Flujo de sincronización Tablero SIAN", PAGE_WIDTH / 2, 740, 20, align="center")

    box_width = 320
    box_height = 70
    start_y = 660
    step = 90
    labels = [
        "Inicio del ciclo programado",
        "retornoxmlmp.py\nFiltra envíos y consulta SOAP",
        "_almacenar_xml\nGuarda/actualiza XML pendientes",
        "historialsian.py\nGenera historial con pre_historial",
        "_marcar_retornomp_procesado\nMarca registros como procesados",
        "Fin del ciclo",
    ]

    centers = []
    for index, label in enumerate(labels):
        center_y = start_y - index * step
        centers.append((PAGE_WIDTH / 2, center_y))
        _draw_rectangle(commands, PAGE_WIDTH / 2 - box_width / 2, center_y - box_height / 2, box_width, box_height, (0.84, 0.91, 0.99), (0.10, 0.32, 0.46))
        _add_text(commands, label, PAGE_WIDTH / 2, center_y + 20, 12, align="center")

    for i in range(len(centers) - 1):
        start = (centers[i][0], centers[i][1] - box_height / 2)
        end = (centers[i + 1][0], centers[i + 1][1] + box_height / 2)
        _draw_arrow(commands, start, end)

    loop_points = [
        (centers[4][0] + box_width / 2, centers[4][1]),
        (centers[4][0] + box_width / 2 + 40, centers[4][1]),
        (centers[1][0] + box_width / 2 + 40, centers[1][1]),
        (centers[1][0] + box_width / 2, centers[1][1]),
    ]
    _draw_polyline_with_arrow(commands, loop_points)
    _add_text(commands, "Nuevos XML pendientes", centers[4][0] + box_width / 2 + 30, centers[2][1] + 10, 10, align="center")

    _add_text(
        commands,
        "Cada ciclo procesa solo registros con cambios recientes y respeta\n"
        "las ventanas de tiempo descritas en resumen.txt.",
        PAGE_WIDTH / 2,
        120,
        11,
        align="center",
    )

    commands.append("Q")
    return commands


def build_use_case_page() -> List[str]:
    commands: List[str] = ["q", "1 w"]
    _add_text(commands, "Casos de uso principales", PAGE_WIDTH / 2, 740, 20, align="center")

    boundary_x = 150
    boundary_y = 160
    boundary_width = PAGE_WIDTH - 2 * boundary_x
    boundary_height = 460
    _draw_rectangle(commands, boundary_x, boundary_y, boundary_width, boundary_height, (0.99, 0.97, 0.91), (0.10, 0.32, 0.46))
    _add_text(commands, "Aplicación Tablero SIAN", PAGE_WIDTH / 2, boundary_y + boundary_height - 30, 13, align="center")

    use_cases = [
        (PAGE_WIDTH / 2, boundary_y + 320, "Ejecutar ciclo\nde sincronización"),
        (PAGE_WIDTH / 2, boundary_y + 220, "Revisar historial\nde notificaciones"),
        (PAGE_WIDTH / 2, boundary_y + 120, "Actualizar parámetros\nde conexión"),
    ]

    for cx, cy, label in use_cases:
        _draw_ellipse(commands, (cx, cy), 110, 45, (0.83, 0.96, 0.89), (0.10, 0.32, 0.46))
        _add_text(commands, label, cx, cy + 15, 12, align="center")

    _draw_actor(commands, (100, boundary_y + 220), "Operador SIAN", align="left")
    _draw_actor(commands, (PAGE_WIDTH - 100, boundary_y + 280), "Ministerio Público", align="right")

    left_connections = [use_cases[0][1], use_cases[1][1], use_cases[2][1]]
    for idx, cy in enumerate(left_connections):
        _draw_arrow(commands, (120, cy), (PAGE_WIDTH / 2 - 110, cy))

    _draw_arrow(commands, (PAGE_WIDTH - 120, use_cases[0][1] + 20), (PAGE_WIDTH / 2 + 110, use_cases[0][1] + 20))

    _add_text(
        commands,
        "Los actores colaboran para mantener sincronizados los estados del SIAN\n"
        "con las notificaciones del Ministerio Público.",
        PAGE_WIDTH / 2,
        150,
        11,
        align="center",
    )

    commands.append("Q")
    return commands


def main() -> None:
    pdf = SimplePDF()
    pdf.add_page(build_flowchart_page())
    pdf.add_page(build_use_case_page())
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    pdf.save(OUTPUT_PATH)
    print(f"Archivo PDF creado en: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
