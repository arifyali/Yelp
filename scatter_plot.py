import pandas as pd

from bokeh.plotting import Figure, show, output_file, ColumnDataSource

df = pd.read_csv('success_vs_housing.csv')
print df.head()

source = ColumnDataSource(data=df)

p = Figure(title='Success Metric vs Housing Costs',
           x_axis_label='$/sq. ft.',
           y_axis_label='success_metric',
           y_range=(0,15),
           tools=['crosshair,resize,reset,save'])

p.circle('2016_02', 'success_metric', legend="success_metric", color='lightblue',alpha=0.4, source=source)


output_file("success_vs_housing.html", title="Success vs Housing cost")
show(p)
