# DS200 - LAB 1: MapReduce với Apache Hadoop

## 1. Cấu trúc thư mục

```
DS200-LAB-1/
├── bai1/                   # Bài 1: Tính Điểm Đánh Giá Trung Bình và Tổng Số Lượt Đánh Giá Cho Mỗi Phim
├── bai2/                   # Bài 2: Phân Tích Đánh Giá Theo Thể Loại
├── bai3/                   # Bài 3: Phân tích đánh giá theo giới tính
├── bai4/                   # Bài 4: Phân tích đánh giá theo nhóm tuổi
├── data/                   # Dữ liệu đầu vào
├── output_image/           # Ảnh kết quả output từng bài
└── assignments.ipynb       # Yêu cầu bài tập
```

---

## 2. Dữ liệu (`data/`)

| File | Schema | Mô tả |
|------|--------|-------|
| `movies.txt` | MovieID, Title, Genres | Danh sách phim |
| `ratings_1.txt` | UserID, MovieID, Rating, Timestamp | Đánh giá phim (phần 1) |
| `ratings_2.txt` | UserID, MovieID, Rating, Timestamp | Đánh giá phim (phần 2) |
| `users.txt` | UserID, Gender, Age, Occupation, Zip-code | Thông tin người dùng |

---

## 3. Các bài tập

### Bài 1: Tính Điểm Đánh Giá Trung Bình và Tổng Số Lượt Đánh Giá Cho Mỗi Phim

**Mục tiêu:** Tính điểm trung bình và tổng số lượt đánh giá cho từng phim từ cả 2 file ratings. Tìm phim có điểm trung bình cao nhất trong số các phim có ít nhất 5 lượt đánh giá.

**Output:**
```
MovieTitle  AverageRating: X.XX (TotalRatings: XX)
...
MovieTitle is the highest rated movie with an average rating of X.XX among movies with at least 5 ratings.
```

**Lưu ý:** Sử dụng biến lớp `maxMovie` và `maxRating` trong Reducer, xuất kết quả phim cao nhất trong `cleanup()`.

Thư mục `bai1/` chứa:
- `MovieRatingMapper.java` — Mapper đọc ratings, emit (MovieID, Rating)
- `MovieRatingReducer.java` — Reducer tính avg, tìm max trong `cleanup()`
- `MovieRatingDriver.java` — Driver, load movies.txt qua DistributedCache
- `MovieRating.jar` — File JAR đã đóng gói
- `build/` — Thư mục chứa các file `.class`

**Cách chạy:**
```bash
hadoop jar MovieRating.jar MovieRatingDriver \
  /input/ratings \
  /input/movies/movies.txt \
  /output/movie_rating

hadoop fs -cat /output/movie_rating/part-r-00000
```

### Bài 2: Phân Tích Đánh Giá Theo Thể Loại

**Mục tiêu:** Tách từng thể loại từ cột Genres (phân tách bằng `|`), tính điểm trung bình và tổng số lượt đánh giá cho từng thể loại.

**Output:** `Genre: AverageRating (TotalRatings)`

Thư mục `bai2/` chứa:
- `GenreRatingMapper.java` — Mapper tách genres, emit (Genre, Rating)
- `GenreRatingReducer.java` — Reducer tính avg theo từng thể loại
- `GenreRatingDriver.java` — Driver, load movies.txt qua DistributedCache
- `GenreRating.jar` — File JAR đã đóng gói
- `build/` — Thư mục chứa các file `.class`

**Cách chạy:**
```bash
hadoop jar GenreRating.jar GenreRatingDriver \
  /input/ratings \
  /output/genre \
  /input/movies/movies.txt

hadoop fs -cat /output/genre/part-r-00000
```

### Bài 3: Phân Tích Đánh Giá Theo Giới Tính

**Mục tiêu:** Join dữ liệu ratings và users theo UserID, tính điểm trung bình theo giới tính (Nam/Nữ) cho từng phim.

**Output:** `MovieTitle: Male_Avg: X.XX, Female_Avg: X.XX`

Thư mục `bai3/` chứa:
- `GenderRatingMapper.java` — Mapper cho Job 1 (join ratings + users)
- `GenderRatingReducer.java` — Reducer cho Job 1
- `GenderRatingMapper2.java` — Mapper cho Job 2
- `GenderRatingReducer2.java` — Reducer cho Job 2 (tính avg)
- `GenderRatingDriver.java` — Driver chạy 2 MapReduce jobs
- `GenderRating.jar` — File JAR đã đóng gói
- `out/` — Thư mục chứa các file `.class`

**Cách chạy:**
```bash
hadoop jar GenderRating.jar GenderRatingDriver \
  /input/ratings \
  /input/users/users.txt \
  /input/movies/movies.txt \
  /output/gender_intermediate \
  /output/gender_final

hadoop fs -cat /output/gender_final/part-r-00000
```

### Bài 4: Phân Tích Đánh Giá Theo Nhóm Tuổi

**Mục tiêu:** Phân nhóm người dùng theo độ tuổi, tính điểm trung bình theo từng nhóm tuổi cho từng phim.

**Nhóm tuổi:** 0-18 | 18-35 | 35-50 | 50+

**Output:** `MovieTitle: [0-18: X.XX, 18-35: X.XX, 35-50: X.XX, 50+: X.XX]`

Thư mục `bai4/` chứa:
- `AgeRatingMapper.java` — Mapper cho Job 1 (join ratings + users)
- `AgeRatingReducer.java` — Reducer cho Job 1
- `AgeRatingMapper2.java` — Mapper cho Job 2
- `AgeRatingReducer2.java` — Reducer cho Job 2 (tính avg theo nhóm tuổi)
- `AgeRatingDriver.java` — Driver chạy 2 MapReduce jobs
- `AgeRating.jar` — File JAR đã đóng gói
- `out/` — Thư mục chứa các file `.class`

**Cách chạy:**
```bash
hadoop jar AgeRating.jar AgeRatingDriver \
  /input/ratings \
  /input/users/users.txt \
  /input/movies/movies.txt \
  /output/age_intermediate \
  /output/age_final

hadoop fs -cat /output/age_final/part-r-00000
```

---

## 4. Môi trường

- **OS:** macOS
- **Hadoop:** Single-node cluster (localhost)
- **Java:** JDK 8+
- **HDFS input:** `/input/ratings/`, `/input/users/`, `/input/movies/`

---

## 5. Hướng dẫn chung

**Upload dữ liệu lên HDFS:**
```bash
hadoop fs -mkdir -p /input/ratings /input/users /input/movies
hadoop fs -put ratings_1.txt ratings_2.txt /input/ratings/
hadoop fs -put users.txt /input/users/users.txt
hadoop fs -put movies.txt /input/movies/movies.txt
```

**Compile và đóng gói JAR:**
```bash
mkdir out
javac -classpath $(hadoop classpath) -d out *.java
jar -cvf <TenFile>.jar -C out .
```

**Xóa output cũ trước khi chạy lại:**
```bash
hadoop fs -rm -r /output/<ten_output>
```

**Xem kết quả:**
```bash
hadoop fs -cat /output/<ten_output>/part-r-00000
```

**Lưu kết quả về máy local:**
```bash
hadoop fs -get /output/<ten_output>/part-r-00000 ~/Desktop/result.txt
```
