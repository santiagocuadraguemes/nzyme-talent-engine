import os
import sys

# Asegura que la raíz del proyecto está en sys.path para poder importar el paquete core
# con independencia de desde dónde se invoque pytest.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Silencia el log de la aplicación durante los tests (los módulos crean loggers a stdout).
os.environ.setdefault("LOG_LEVEL", "CRITICAL")
