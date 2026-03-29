```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.22631.6199/23H2/2023Update/SunValley3)
Intel Core i9-14900K, 1 CPU, 32 logical and 24 physical cores
.NET SDK 9.0.311
  [Host]     : .NET 8.0.24 (8.0.2426.7010), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.24 (8.0.2426.7010), X64 RyuJIT AVX2


```
| Method       | Mean        | Error    | StdDev   | Gen0   | Gen1   | Allocated |
|------------- |------------:|---------:|---------:|-------:|-------:|----------:|
| PointLookup  | 3,062.98 μs | 8.533 μs | 7.982 μs |      - |      - |       2 B |
| RangeScan_1K |    17.04 μs | 0.093 μs | 0.087 μs | 0.8850 | 0.0305 |   16664 B |
