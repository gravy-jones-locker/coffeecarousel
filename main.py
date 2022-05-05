import sys
import os

from src.carousel import Carousel

def main():
    Carousel().spin()

if __name__ == '__main__':
    os.environ["DEBUG_CAROUSEL"] = str('--deploy' not in sys.argv)
    main()