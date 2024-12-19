import board
import neopixel
print("NeoPixel")
stat_pixel = neopixel.NeoPixel(board.D18, 1, brightness=0.3, auto_write=True)

stat_pixel.fill((0,255,0))

print("End")
