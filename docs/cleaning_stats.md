# Thống kê làm sạch (Ngày 3)

Nguồn: `data/2019-Oct.csv`

| Bước | Số dòng | % so với đầu vào |
| --- | ---: | ---: |
| Dòng đầu vào | 42,448,764 | 100% |
| Loại: thiếu user_id/product_id/event_time/event_type/price | 0 | 0.0000% |
| Loại: event_type không hợp lệ | 0 | 0.0000% |
| Loại: price không phải số | 0 | 0.0000% |
| Loại: price <= 0 | 68,673 | 0.1618% |
| Loại: thời gian không đọc được | 0 | 0.0000% |
| Loại: trùng lặp (trong từng khối 1 triệu dòng) | 30,217 | 0.0712% |
| **events_clean.csv** | **42,349,874** | 99.77% |

Giá trị thiếu được điền `unknown` (không loại dòng):

- category_code: 13,478,561 dòng (31.75%)
- brand: 6,048,407 dòng (14.25%)

price <= 0 theo loại sự kiện: view=68,523, cart=150

## Theo loại sự kiện

| event_type | Trước | Sau |
| --- | ---: | ---: |
| view | 40,779,399 | 40,708,807 |
| cart | 926,516 | 898,294 |
| purchase | 742,849 | 742,773 |

## transactions.csv (chỉ purchase)

- Purchase sau làm sạch trong events: 742,773
- Trùng lặp toàn cục bị loại: 0
- **transactions.csv: 742,773 dòng**, 347,118 khách, 42,241 sản phẩm
- Khoảng thời gian: 2019-10-01 00:02:14 đến 2019-10-31 23:59:16
