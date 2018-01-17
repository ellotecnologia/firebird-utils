from distutils.core import setup
import py2exe

import sys; sys.argv.append('py2exe')

py2exe_options = dict(
                      excludes=['_ssl',  # Exclude _ssl
                                'pyreadline', 'difflib', 'doctest', 
                                'optparse', 'pickle', 'calendar', 'pdb', 'inspect', 'unittest'],  # Exclude standard library
                      dll_excludes=['msvcr71.dll'],  # Exclude msvcr71
                      compressed=True,  # Compress library.zip
                      bundle_files=1
                      )

setup(name='Schema Equalizer',
      version='1.0',
      description='<Description>',
      author='Clayton A. Alves',
      options={'py2exe': py2exe_options},
      console=[{
          'script': 'schema_equalizer.py',
          "icon_resources": [(1, "ello.ico")]
          }],
      zipfile=None
      )
