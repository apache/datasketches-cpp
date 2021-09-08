from .count_sketch import CountSketch

try:
  from _datasketches import *
except ImportError:
  print("Import error!")
