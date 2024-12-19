import board
import digitalio
from PIL import Image, ImageDraw, ImageFont
import adafruit_ssd1306


# Define the Reset Pin
oled_reset = digitalio.DigitalInOut(board.D4)

# Change these
# to the right size for your display!
WIDTH = 128
HEIGHT = 64  # Change to 64 if needed
BORDER = 5

 # Use for I2C.
i2c = board.I2C()  # uses board.SCL and board.SDA
# i2c = board.STEMMA_I2C()  # For using the built-in STEMMA QT connector on a microcontroller
oled = adafruit_ssd1306.SSD1306_I2C(WIDTH, HEIGHT, i2c, addr=0x3C, reset=oled_reset)

# Create blank image for drawing.
# Make sure to create image with mode '1' for 1-bit color.
image = Image.new("1", (oled.width, oled.height))

# Load default font.
font = ImageFont.load_default()


def welcome_splash():

    # Clear display.
    oled.fill(0)
    oled.show()

    # Create blank image for drawing.
    # Make sure to create image with mode '1' for 1-bit color.
    # image = Image.new("1", (oled.width, oled.height))


    draw = ImageDraw.Draw(image)

    # Draw a white background
    draw.rectangle((0, 0, oled.width, oled.height), outline=255, fill=255)

    # Draw a smaller inner rectangle
    draw.rectangle(
        (BORDER, BORDER, oled.width - BORDER - 1, oled.height - BORDER - 1),
        outline=0,
        fill=0,
    )

    
    # Draw Some Text
    text = "GEDI Datalogger V3"
    bbox = font.getbbox(text)
    (font_width, font_height) = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text(
        (oled.width // 2 - font_width // 2, oled.height // 2 - font_height // 2),
        text,
        font=font,
        fill=255,
    )

    oled.image(image)
    oled.show()

    # return Image

def disp_time():
    from datetime import datetime
    clock = datetime.strftime(datetime.today(), "%H:%M %p")
    draw = ImageDraw.Draw(image)
    bbox = font.getbbox(clock)

    draw.text(
        (oled.width-bbox[2]+bbox[0]-7,0),
        clock,
        font=font,
        fill=255,
    )
    oled.image(image)
    oled.show()

def disp_sensor(tx_details):
    
    s_id={}
    rssi={}
    s_id["text"]=tx_details.split(":")[0]
    rssi["text"]=tx_details.split(":")[1]+"dBm"

    draw = ImageDraw.Draw(image)

    s_id["font"] = ImageFont.truetype("DejaVuSansMono-Bold.ttf", 30)
    s_id["bbox"] = s_id["font"].getbbox(s_id["text"])

    print(oled.height/2-(s_id["bbox"][3] - s_id["bbox"][1]))

    draw.text(
    (0, oled.height/2-(s_id["bbox"][3] - s_id["bbox"][1])),
        # (0,32),
        s_id["text"],
        font=s_id["font"],
        fill=255,
    )

    rssi["font"] = ImageFont.truetype("DejaVuSansMono-Bold.ttf", 10)        
    rssi["bbox"] = rssi["font"].getbbox(rssi["text"])

    draw.text(
        (s_id["bbox"][2]+5, oled.height/2-(s_id["bbox"][3] - s_id["bbox"][1])+5),
        # (s_id["bbox"][2], 10),
        # (0,32),
        rssi["text"],
        font=rssi["font"],
        fill=255,
    )
    
    # oled.text(s_id, oled.width/2-(bbox[2] - bbox[0]), oled.height/2-(bbox[3] - bbox[1]), 1)

    oled.image(image)
    oled.show()

def disp_tips(tips):
    
    draw = ImageDraw.Draw(image)

    font = ImageFont.truetype("DejaVuSansMono-Bold.ttf", 30)
    text = str(tips)
    bbox = font.getbbox(text)

    draw.text(
        (0, oled.height/2-(bbox[3] - bbox[1])),
        text,
        font=font,
        fill=255,
    )

    draw.text(
        (bbox[2]+5, oled.height/2-(bbox[3] - bbox[1])+5),
        " tips",
        font=ImageFont.truetype("DejaVuSansMono-Bold.ttf", 10),
        fill=255,
    )
    
    oled.image(image)
    oled.show()


def get_arguments():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-w","--welcome", action='store_true', help="welcome splash screen")
    parser.add_argument("-c","--clock", action='store_true', help="display clock")
    parser.add_argument("-s","--sensor", help="sensor transmission")
    # parser.add_argument("-b","--bu_id", help="business unit id", type=int)
    # parser.add_argument("-c","--save_to_csv", help="save to csv", action='store_true')
    # parser.add_argument("-t","--type", help="plot type")
    parser.add_argument("-t","--tips", help="rain tips")

    args = parser.parse_args()

    return args

def main():

    args=get_arguments()

    if args.welcome:
        welcome_splash()

    if args.clock:
        disp_time()

    if args.sensor:
        disp_sensor(args.sensor)

    if args.tips:
        disp_tips(args.tips)



if __name__ == "__main__":
    main()