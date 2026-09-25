# HFT backtesting engine baseline

Proyek ini adalah baseline pembelajaran untuk memisahkan jalur panas matching
dari transport dan analitik. Go menjalankan LOB dan matching secara deterministik;
Python menghasilkan tick/order dan menghitung PnL.

## Struktur

| Path | Peran |
| --- | --- |
| `engine/orderbook.go` | price ladder, FIFO queue, add/match/cancel |
| `engine/price_index.go` | hierarchical bitmap untuk best bid/ask |
| `engine/order.go` | intrusive `Order` dan `sync.Pool` |
| `engine/engine.go` | bounded inbox + single-writer goroutine |
| `ipc/protocol.go` | frame biner fixed-size 26/34 byte |
| `ipc/server.go` | TCP reader/writer per koneksi |
| `cmd/hftd/main.go` | daemon |
| `strategy_pump.py` | market maker dan PnL |

## Mengapa rentang tick dibatasi

Harga dipetakan langsung ke array `index = priceTicks - minPriceTicks`.
Hash map `orderID -> *Order` memberi lookup cancel O(1), sedangkan doubly
linked list pada `PriceLevel` memberi append, FIFO head removal, dan cancel node
O(1). Bitmap bertingkat menyimpan harga aktif; set/clear/best-price menyentuh
`log_64(range)` word yang tetap kecil dan tidak bergantung pada jumlah order.
Ini adalah bentuk praktis O(1) untuk domain tick yang dikonfigurasi. Jika harga
tidak dapat dibatasi, ganti ladder dengan B-tree/skip-list dan terima O(log P)
untuk aktivasi price level.

## Concurrency dan race condition

Semua koneksi TCP boleh mengirim bersamaan ke channel inbox, tetapi hanya satu
goroutine `Engine.Run` yang memanggil `OrderBook.Process`. Karena map, bitmap,
dan pointer list memiliki satu pemilik, jalur matching tidak membutuhkan
`sync.Mutex` dan tidak mengalami race antar-mutation. Setiap koneksi memiliki
satu goroutine pembaca dan satu penulis; tidak ada dua goroutine yang memanggil
`Write` pada `net.Conn` yang sama. Channel `Done` membatalkan pengiriman hasil
ketika client putus sehingga goroutine matching tidak tersandera socket mati.

Jika book harus diakses langsung dari beberapa goroutine, lindungi seluruh
operasi mutasi dengan `sync.Mutex`. `sync.RWMutex` hanya cocok untuk snapshot
read-heavy yang pendek; writer tetap harus mengambil write lock selama update
intrusive list, dan pembacaan pointer tanpa lock tetap merupakan data race.

## Memory dan allocation control

`Order` dipinjam dari `sync.Pool`, di-reset, dimasukkan ke map/list, lalu
dikeluarkan dari semua index sebelum `releaseOrder` menghapus pointer dan
mengembalikannya ke pool. Event yang melintasi channel berisi scalar value,
sehingga tidak ada pooled pointer yang lolos ke goroutine lain. Slice event
adalah alokasi boundary yang sengaja mudah diganti dengan callback/ring buffer
saat benchmark membutuhkan zero-allocation end-to-end. `sync.Pool` dapat
mengosongkan cache saat GC, jadi ini mengurangi tekanan alokasi tetapi bukan
jaminan object selalu tersedia. Untuk produksi, ukur dengan `go test -benchmem`,
`pprof`, dan latency histogram sebelum memilih ukuran queue/rentang tick.

## Menjalankan

```bash
go test ./...
go run ./cmd/hftd -listen 127.0.0.1:9000 -min-price-ticks 0 -max-price-ticks 1000000
python3 strategy_pump.py --ticks 100 --spread 2 --qty 1
```

Frame order Python memakai `struct !BQbqq`: type, uint64 order ID, side (0 buy,
1 sell), int64 price tick, int64 quantity. Go membalas `!BQQqqb`: event type,
taker ID, maker ID, price, quantity, dan side (atau reject code pada event
reject). TCP localhost dipilih untuk baseline yang mudah di-debug; benchmark
serius dapat mengganti `ipc` dengan shared-memory SPSC ring atau UDP feed yang
memiliki loss/replay policy eksplisit.

## Batasan yang sengaja terlihat

Tidak ada self-trade prevention, fee/rebate, sequence gap recovery, persistence,
clock synchronization, risk limits, atau exchange-specific matching flags.
`strategy_pump.py` dapat menyuntikkan taker flow sintetis agar fill dan PnL
terlihat saat daemon dijalankan sendiri; matikan dengan `--no-inject-taker`
saat memakai aliran historis eksternal.
