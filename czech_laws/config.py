from pathlib import Path
import __main__

class Config:
    output_dir = Path(__main__.__file__).resolve().parent