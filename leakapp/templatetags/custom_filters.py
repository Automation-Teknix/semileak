from django import template

register = template.Library()

@register.filter(name='color_palette')
def color_palette(index):
    colors = [
'#0d6efd', # Primary (bg-primary)
'#198754' , # Indigo (deep purple-blue)
'#0dcaf0',# Info (bg-info)
'#ffc107', # Warning (bg-warning)
'#461b35c7', # Danger (bg-danger)
'#ef570ed6', # Secondary (bg-secondary)
'#6c757d', # Custom Purple (explicitly defined in style attribute)
'#5e418c', # Primary (bg-primary)
'#0d6efd', # Success (bg-success)
'#198754', # Info (bg-info)
'#0dcaf0', # Warning (bg-warning)
'#ffc107', # Danger (bg-danger)
'#461b35c7', # Secondary (bg-secondary)
'#ef570ed6',
'#6c757d',
'#5e418c', 
    ]
    return colors[index % len(colors)]
