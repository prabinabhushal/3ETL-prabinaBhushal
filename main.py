import sys
from Extract1st.Extract import extract
from ScrubExplode2nd.Scrub import scrub
from LoadSQL.ETLloadpsyco import load

def main():
    if len(sys.argv) == 1:
        sys.exit(1)

    if "extract" in sys.argv:
        print("Running extract...")
        extract()

    if "scrub" in sys.argv:
        print("Running scrub...")
        scrub()

    if "load" in sys.argv:
        print("Running load...")
        load()

if __name__ == "__main__":
    main()
