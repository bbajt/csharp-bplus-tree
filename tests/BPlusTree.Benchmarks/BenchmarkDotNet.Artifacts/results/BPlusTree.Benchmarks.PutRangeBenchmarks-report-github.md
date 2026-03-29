```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.22631.6199/23H2/2023Update/SunValley3)
Intel Core i9-14900K, 1 CPU, 32 logical and 24 physical cores
.NET SDK 9.0.311
  [Host]     : .NET 8.0.24 (8.0.2426.7010), X64 RyuJIT AVX2
  Job-KIDHTA : .NET 8.0.24 (8.0.2426.7010), X64 RyuJIT AVX2

InvocationCount=1  IterationCount=5  UnrollFactor=1  
WarmupCount=1  

```
| Method             | Mean        | Error      | StdDev    | Ratio | RatioSD | Gen0      | Gen1      | Gen2      | Allocated   | Alloc Ratio |
|------------------- |------------:|-----------:|----------:|------:|--------:|----------:|----------:|----------:|------------:|------------:|
| AutoCommit_Put_1K  |    21.72 ms |   2.458 ms |  0.638 ms |  1.00 |    0.00 |         - |         - |         - |   189.03 KB |        1.00 |
| PutRange_1K        |    85.80 ms |  62.835 ms | 16.318 ms |  3.96 |    0.79 |         - |         - |         - |  8994.56 KB |       47.58 |
| AutoCommit_Put_10K |   136.00 ms | 122.432 ms | 31.795 ms |  6.28 |    1.57 |         - |         - |         - |   636.84 KB |        3.37 |
| PutRange_10K       | 1,704.96 ms | 318.969 ms | 82.835 ms | 78.55 |    4.06 | 5000.0000 | 5000.0000 | 1000.0000 | 91778.38 KB |      485.52 |
