# Test:

for i in {0..9}; do 
   curl -X POST http://localhost:15000/add_job \
   -H "Content-Type: application/json" \
   -d {id: $i, src: L2Zvbwo=, dst: L2Jhcgo=};
 done
for i in {0..9}; do curl -X POST http://localhost:15000/add_job -H "Content-Type: application/json" -d {id: $i, src: L2Zvbwo=, dst: L2Jhcgo=}; done
for i in {0..9}; do curl -X POST http://localhost:15000/add_job -H "Content-Type: application/json" -d {id: $i, src: L2Zvbwo=, dst: L2Jhcgo=}; done
