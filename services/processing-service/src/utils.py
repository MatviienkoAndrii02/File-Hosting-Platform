from PIL import Image
import io

def create_thumbnail(file_bytes: bytes, size=(128, 128)) -> bytes:
    with Image.open(io.BytesIO(file_bytes)) as img:
        img.thumbnail(size)
        output = io.BytesIO()
        img.save(output, format=img.format)
        return output.getvalue()