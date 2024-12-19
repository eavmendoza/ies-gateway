import board
import digitalio
from PIL import Image, ImageDraw, ImageFont
import adafruit_ssd1306
import busio
import adafruit_gfx as gfx
import time

i2c = busio.I2C(board.SCL, board.SDA)

WIDTH = 128
HEIGHT = 64  # Change to 64 if needed
BORDER = 5
# i2c = I2C(1)

# display = adafruit_ssd1306.SSD1306_I2C(128, 32, i2c)

# display.fill(0)

# display.show()

# # Set a pixel in the origin 0,0 position.
# display.pixel(0, 0, 1)
# # Set a pixel in the middle 64, 16 position.
# display.pixel(64, 16, 1)
# # Set a pixel in the opposite 127, 31 position.
# display.pixel(127, 31, 1)
# display.show()

oled = adafruit_ssd1306.SSD1306_I2C(WIDTH, HEIGHT, i2c, addr=0x3C)

# Use for SPI
# spi = board.SPI()
# oled_cs = digitalio.DigitalInOut(board.D5)
# oled_dc = digitalio.DigitalInOut(board.D6)
# oled = adafruit_ssd1306.SSD1306_SPI(WIDTH, HEIGHT, spi, oled_dc, oled_reset, oled_cs)

# Clear display.
oled.fill(0)
oled.show()

# Create blank image for drawing.
# Make sure to create image with mode '1' for 1-bit color.
image = Image.new("1", (oled.width, oled.height))

# Get drawing object to draw on image.
draw = ImageDraw.Draw(image)

# Draw a white background
draw.rectangle((0, 0, oled.width, oled.height), outline=255, fill=255)

# Draw a smaller inner rectangle
draw.rectangle(
    (BORDER, BORDER, oled.width - BORDER - 1, oled.height - BORDER - 1),
    outline=0,
    fill=0,
)

# Load default font.
font = ImageFont.load_default()

# Draw Some Text
text = "Hello World!"
(font_width, font_height) = font.getsize(text)
draw.text(
    (oled.width // 2 - font_width // 2, oled.height // 2 - font_height // 2),
    text,
    font=font,
    fill=255,
)

# Display image
oled.image(image)
oled.show()

# Optionally create faster horizontal and vertical line drawing functions using
# the display's native filled rectangle function (which updates chunks of memory
# instead of pixel by pixel).
def fast_hline(x, y, width, color):
    display.fill_rectangle(x, y, width, 1, color)


def fast_vline(x, y, height, color):
    display.fill_rectangle(x, y, 1, height, color)
