set style data histogram
set xlabel "Metadata, Data Page Size"
set ylabel "Time (Seconds)"
set yr [0:6000]
set title "hdf5-iotest Hermes VFD 1MB RAM+NVME+BB"
set style fill solid
plot "hdf5-iotest_Hermes_VFD_1MB_RAM_NVME_BB.txt" using 2:xtic(1) linecolor "blue" title "Random",\
'' u 3 linecolor "orange" title "Round-robin"
