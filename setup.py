from distutils.core import setup
import py2exe

setup(
    options = {
        'py2exe': {
            'bundle_files': 1, 
            'compressed': True,

            'excludes' : ['_ssl', 'doctest', 'pdb', 'unittest', 'difflib', 'inspect', 'msvcr71.dll'],

        },
    },

    console=[{
        'script': 'schema_equalizer.py',
        "icon_resources": [(1, "ello.ico")]
    }],

    zipfile = None,
)

