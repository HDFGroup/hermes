set style data histogram
set xlabel "Metadata, Data Page Size"
set ylabel "Time (Seconds)"
set yr [0:4000]
set title "hdf5-iotest Hermes VFD 128KB RAM+NVME+BB"
set style fill solid
plot "hdf5-iotest_Hermes_VFD_128KB_RAM_NVME_BB.txt" using 2:xtic(1) linecolor "blue" title "Random",\
'' u 3 linecolor "orange" title "Round-robin"
