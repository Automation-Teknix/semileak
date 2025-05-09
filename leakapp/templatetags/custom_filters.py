from django import template

register = template.Library()

@register.filter(name='color_palette')
def color_palette(index):
    colors = [
        '#0d6efd' ,  # blue
        '#198754',  # yellow
        '#0dcaf0',  # purple
        '#ffc107',  # teal
        '#dc3545',  # orange
        '#17a2b8',  # cyan
        '#6c757d',  # indigo
        '#5e418c',  # pink
        '#6c757d',  # gray
        '#343a40'   # dark
    ]
    return colors[index % len(colors)]
