import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import zipfile

def build_zip():
    dist_dir = os.path.dirname(os.path.abspath(__file__))
    backend_dir = os.path.join(dist_dir, "backend")
    output_zip = os.path.join(dist_dir, "janstream_lambda.zip")

    if os.path.exists(output_zip):
        os.remove(output_zip)

    print("📦 Packaging pure-Python JanStream Lambda package...")

    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        # 1. Add lambda_function.py at root
        lf_path = os.path.join(backend_dir, "lambda_function.py")
        if os.path.exists(lf_path):
            zf.write(lf_path, "lambda_function.py")
            print("  + Added lambda_function.py (root handler)")

        # 2. Add app/ directory
        app_dir = os.path.join(backend_dir, "app")
        for root, dirs, files in os.walk(app_dir):
            for file in files:
                if file.endswith((".py", ".json")):
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, backend_dir)
                    zf.write(full_path, rel_path)
                    print(f"  + Added {rel_path}")

    size_kb = os.path.getsize(output_zip) / 1024.0
    print(f"\n✅ Build complete: {output_zip} ({size_kb:.1f} KB)")
    print("👉 100% pure Python! Zero C-extension issues on Amazon Linux!")

if __name__ == "__main__":
    build_zip()
