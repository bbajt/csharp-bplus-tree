```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.22631.6199/23H2/2023Update/SunValley3)
Intel Core i9-14900K, 1 CPU, 32 logical and 24 physical cores
.NET SDK 9.0.311
  [Host]     : .NET 8.0.24 (8.0.2426.7010), X64 RyuJIT AVX2
  Job-LIPURB : .NET 8.0.24 (8.0.2426.7010), X64 RyuJIT AVX2

InvocationCount=1  IterationCount=5  UnrollFactor=1  
WarmupCount=1  

```
| Method         | Mean    | Error   | StdDev  | Gen0      | Gen1      | Allocated |
|--------------- |--------:|--------:|--------:|----------:|----------:|----------:|
| FullCompaction | 17.97 s | 11.12 s | 2.888 s | 5000.0000 | 1000.0000 | 105.61 MB |
