// (1) Lấy tất cả các trận đấu có player2 tag là 2RU0VU8UP
player2_matches = from(bucket: "primary")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "game_results")
  |> filter(fn: (r) => r["_field"] == "player2_tag")
  |> filter(fn: (r) => r["_value"] == "2RU0VU8UP")

// (2) Lấy trophies của player2
player2_trophies = from(bucket: "primary")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "game_results")
  |> filter(fn: (r) => r["_field"] == "player2_trophies")

// (3) Lấy tất cả các trận đấu có player1 tag là 2RU0VU8UP
player1_matches = from(bucket: "primary")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "game_results")
  |> filter(fn: (r) => r["_field"] == "player1_tag")
  |> filter(fn: (r) => r["_value"] == "2RU0VU8UP")

// (4) Lấy trophies của player1
player1_trophies = from(bucket: "primary")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "game_results")
  |> filter(fn: (r) => r["_field"] == "player1_trophies")

// (5) Kết hợp dữ liệu của player2
player2_data = join(
  tables: {matches: player2_matches, trophies: player2_trophies},
  on: ["_time"]
)
|> map(fn: (r) => ({
    _time: r._time,
    _value: r._value_trophies
  }))

// (6) Kết hợp dữ liệu của player1
player1_data = join(
  tables: {matches: player1_matches, trophies: player1_trophies},
  on: ["_time"]
)
|> map(fn: (r) => ({
    _time: r._time,
    _value: r._value_trophies
  }))

// (7) Gộp dữ liệu của player1 và player2
union(tables: [player1_data, player2_data])
|> sort(columns: ["_time"], desc: false) // Sắp xếp theo thời gian tăng dần
|> yield(name: "combined_players")
