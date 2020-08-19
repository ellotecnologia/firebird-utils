@echo off
pyinstaller --onefile --noupx --hidden-import fdb schema_equalizer.py 
