"""Build a short captioned demo from cropped screenshots of the test groups."""

from pathlib import Path
import subprocess

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
STILLS = ROOT / "stills"
OUT = ROOT / "telegram-max-montage.mp4"
SIZE = (1920, 1080)
FONT = "/System/Library/Fonts/Supplemental/Arial.ttf"
FONT_BOLD = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
BG = "#0B1020"
WHITE = "#F6F8FF"
MUTED = "#B6BED2"
TG = "#32A9E0"
MAX = "#8A63F2"


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(FONT_BOLD if bold else FONT, size)


def canvas() -> tuple[Image.Image, ImageDraw.ImageDraw]:
    im = Image.new("RGB", SIZE, BG)
    draw = ImageDraw.Draw(im)
    return im, draw


def title(draw: ImageDraw.ImageDraw, heading: str, subheading: str) -> None:
    draw.text((76, 55), heading, fill=WHITE, font=font(62, True))
    draw.text((79, 137), subheading, fill=MUTED, font=font(31))


def card(
    im: Image.Image,
    draw: ImageDraw.ImageDraw,
    source: Image.Image,
    box: tuple[int, int, int, int],
    crop: tuple[int, int, int, int],
    label: str,
    color: str,
) -> None:
    x0, y0, x1, y1 = box
    draw.rounded_rectangle((x0 + 6, y0 + 9, x1 + 6, y1 + 9), radius=26, fill="#060915")
    draw.rounded_rectangle(box, radius=26, fill="#172036")
    draw.rounded_rectangle((x0, y0, x1, y0 + 80), radius=26, fill="#172036")
    draw.rectangle((x0, y0 + 48, x1, y0 + 80), fill="#172036")
    draw.ellipse((x0 + 27, y0 + 25, x0 + 47, y0 + 45), fill=color)
    draw.text((x0 + 61, y0 + 18), label, fill=WHITE, font=font(32, True))
    inner = (x0 + 18, y0 + 91, x1 - 18, y1 - 19)
    panel_w = inner[2] - inner[0]
    panel_h = inner[3] - inner[1]
    piece = source.crop(crop)
    scale = min(panel_w / piece.width, panel_h / piece.height)
    piece = piece.resize((round(piece.width * scale), round(piece.height * scale)), Image.Resampling.LANCZOS)
    px = inner[0] + (panel_w - piece.width) // 2
    py = inner[1] + (panel_h - piece.height) // 2
    im.paste(piece, (px, py))


def footer(draw: ImageDraw.ImageDraw, text: str) -> None:
    draw.text((78, 1018), text, fill=MUTED, font=font(25))


tg = Image.open(STILLS / "tg_existing_crop.png").convert("RGB")
max_chat = Image.open(STILLS / "max_existing_crop.png").convert("RGB")

# The screenshots were cropped before this script runs: they contain only the two test chats.
frames: list[tuple[Path, int]] = []

im, draw = canvas()
draw.rounded_rectangle((75, 217, 1845, 788), radius=45, fill="#172036")
draw.ellipse((182, 335, 332, 485), fill=TG)
draw.text((212, 373), "TG", fill=WHITE, font=font(48, True))
draw.text((379, 373), "↔", fill=WHITE, font=font(72, True))
draw.ellipse((518, 335, 668, 485), fill=MAX)
draw.text((543, 373), "MAX", fill=WHITE, font=font(36, True))
draw.text((180, 545), "Локальный мост Telegram ↔ MAX", fill=WHITE, font=font(62, True))
draw.text((181, 640), "Реальные сообщения в двух тестовых группах", fill=MUTED, font=font(35))
footer(draw, "Демонстрация по кадрам переписки · 19.10.2026")
p = STILLS / "01-title.png"
im.save(p)
frames.append((p, 3))

im, draw = canvas()
title(draw, "Telegram → MAX", "Участник пишет в Telegram, ответ приходит из MAX")
card(im, draw, tg, (75, 247, 927, 940), (22, 505, 1010, 1130), "Telegram · исходный чат", TG)
card(im, draw, max_chat, (993, 247, 1845, 940), (335, 126, 1690, 707), "MAX · копия и ответ", MAX)
draw.text((940, 554), "→", fill=WHITE, font=font(42, True))
footer(draw, "Бот показывает автора. Ответ привязан к исходному сообщению.")
p = STILLS / "02-tg-to-max.png"
im.save(p)
frames.append((p, 13))

im, draw = canvas()
title(draw, "MAX → Telegram", "Участник пишет в MAX, ответ возвращается из Telegram")
card(im, draw, max_chat, (75, 247, 927, 940), (245, 697, 1680, 1168), "MAX · исходный чат", MAX)
card(im, draw, tg, (993, 247, 1845, 940), (24, 1137, 823, 1435), "Telegram · копия и ответ", TG)
draw.text((940, 554), "→", fill=WHITE, font=font(42, True))
footer(draw, "Ответ Telegram → MAX подтверждён в тестовой группе.")
p = STILLS / "03-max-to-tg.png"
im.save(p)
frames.append((p, 13))

im, draw = canvas()
draw.text((110, 262), "Сообщения и ответы", fill=WHITE, font=font(82, True))
draw.text((110, 373), "работают в обе стороны", fill=WHITE, font=font(82, True))
draw.rounded_rectangle((110, 570, 1810, 728), radius=28, fill="#172036")
draw.text((156, 610), "Telegram  ↔  MAX", fill="#BFCFFF", font=font(53, True))
footer(draw, "Локальный прототип · серверное размещение ещё не выполнено")
p = STILLS / "04-end.png"
im.save(p)
frames.append((p, 5))

concat = ROOT / "stills" / "concat.txt"
concat.write_text(
    "".join(f"file '{path.name}'\nduration {duration}\n" for path, duration in frames)
    + f"file '{frames[-1][0].name}'\n",
    encoding="utf-8",
)
subprocess.run(
    [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(concat),
        "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
        "-vf", "fps=30,format=yuv420p", "-c:v", "libx264", "-preset", "medium",
        "-crf", "20", "-c:a", "aac", "-b:a", "96k", "-shortest", "-movflags", "+faststart",
        str(OUT),
    ],
    check=True,
)
print(OUT)
