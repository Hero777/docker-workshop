from pathlib import Path

# Определяем путь к текущей папке и имя самого скрипта
current_dir = Path.cwd()
current_file = Path(__file__).name

print(f"Files in {current_dir}:")

# Перебираем все объекты в текущей директории
for filepath in current_dir.iterdir():
    # Пропускаем сам файл скрипта, чтобы он не читал свой собственный код
    if filepath.name == current_file:
        continue

    print(f"  - {filepath.name}")

    # Проверяем, что объект — это файл, а не папка
    if filepath.is_file():
        try:
            # Читаем содержимое текстового файла
            content = filepath.read_text(encoding='utf-8')
            print(f"    Content: {content}")
        except Exception as e:
            # Если файл не текстовый (например, картинка), выводим ошибку
            print(f"    Could not read file: {e}")