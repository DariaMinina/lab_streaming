import math

def calculate_epicenter(x1, y1, intensity1, x2, y2, intensity2):
    # Расчет расстояний
    dx = x2 - x1
    dy = y2 - y1
    
    # Формула бисектрисы
    x = ((intensity1 * y2 - intensity2 * y1) / (dx**2 + dy**2)) * math.sqrt(dx**2 + dy**2)
    y = ((intensity2 * x1 - intensity1 * x2) / (dx**2 + dy**2)) * math.sqrt(dx**2 + dy**2)
    
    return x, y

# Пример использования
epicenter_x, epicenter_y = calculate_epicenter(30.0, 60.0, 8, 31.0, 59.0, 9)

print(f"Координаты эпицентра: ({epicenter_x}, {epicenter_y})")