import pandas as pd

from bokeh.plotting import Figure, show, output_file, ColumnDataSource
from bokeh.models import HoverTool

df = pd.read_csv('mean_success_housing.csv')
df['success_metric'] = df['_c0']
print df.head()


source1 = ColumnDataSource(data=df)
hover1 = HoverTool(tooltips=[("zipcode","@zipcode"),
                             ("success_metric","@success_metric")])

p = Figure(title='Mean Success by zip Success Metric vs Housing Costs',
           x_axis_label='$/sq. ft.',
           y_axis_label='success_metric',
           tools=['crosshair,resize,reset,save', hover1])

p.circle('2016_02', 'success_metric', color='red',alpha=0.4, source=source1)

output_file("mean_success_housing.html", title="Success vs Housing cost")
show(p)
