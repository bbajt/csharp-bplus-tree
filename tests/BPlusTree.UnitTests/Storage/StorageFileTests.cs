using System;
using System.IO;
using Xunit;
using ByTech.BPlusTree.Core.Api;
using ByTech.BPlusTree.Core.Storage;

namespace ByTech.BPlusTree.Core.Tests.Storage;

public class StorageFileTests : IDisposable
{
    private readonly string _testFilePath;
    private readonly int _pageSize = 8192;

    public StorageFileTests()
    {
        _testFilePath = Path.Combine(Path.GetTempPath(), $"StorageFileTest_{Guid.NewGuid()}.dat");
    }

    public void Dispose()
    {
        if (File.Exists(_testFilePath))
        {
            File.Delete(_testFilePath);
        }
    }

    [Fact]
    public void StorageFile_Ctor_CreatesNewFile()
    {
        using var storageFile = new StorageFile(_testFilePath, _pageSize, createNew: true);
        Assert.True(File.Exists(_testFilePath));
        Assert.Equal(0u, storageFile.TotalPageCount);
    }

    [Fact]
    public void StorageFile_Ctor_OpensExistingFile()
    {
        // Create a file first
        File.WriteAllText(_testFilePath, "test data");

        using var storageFile = new StorageFile(_testFilePath, _pageSize, createNew: false);
        Assert.True(File.Exists(_testFilePath));
    }

    [Fact]
    public void StorageFile_ReadPage_ThrowsForNonExistentPage()
    {
        using var storageFile = new StorageFile(_testFilePath, _pageSize, createNew: true);

        Assert.Throws<PageNotFoundException>(() => storageFile.ReadPage(0));
    }

    [Fact]
    public void StorageFile_WritePage_ReadPage_RoundTrip()
    {
        using var storageFile = new StorageFile(_testFilePath, _pageSize, createNew: true);

        // Write a page
        var testData = new byte[_pageSize];
        for (int i = 0; i < _pageSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        storageFile.WritePage(0, testData);

        // Read it back
        var readData = storageFile.ReadPage(0);

        Assert.Equal(testData, readData.ToArray());
    }

    [Fact]
    public void StorageFile_AllocatePage_ExtendsFile()
    {
        using var storageFile = new StorageFile(_testFilePath, _pageSize, createNew: true);

        var pageId1 = storageFile.AllocatePage();
        var pageId2 = storageFile.AllocatePage();

        Assert.Equal(0u, pageId1);
        Assert.Equal(1u, pageId2);
        Assert.Equal(2u, storageFile.TotalPageCount);
    }

    [Fact]
    public void StorageFile_WritePage_ExtendsFile()
    {
        using var storageFile = new StorageFile(_testFilePath, _pageSize, createNew: true);

        // Write to page 5 (should extend file)
        var testData = new byte[_pageSize];
        for (int i = 0; i < _pageSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        storageFile.WritePage(5, testData);

        Assert.Equal(6u, storageFile.TotalPageCount);
    }

    [Fact]
    public void StorageFile_WritePage_ThrowsForWrongSize()
    {
        using var storageFile = new StorageFile(_testFilePath, _pageSize, createNew: true);

        var testData = new byte[_pageSize + 1];
        Assert.Throws<ArgumentException>(() => storageFile.WritePage(0, testData));
    }

    [Fact]
    public void StorageFile_Flush_WritesChanges()
    {
        using var storageFile = new StorageFile(_testFilePath, _pageSize, createNew: true);

        var testData = new byte[_pageSize];
        for (int i = 0; i < _pageSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        storageFile.WritePage(0, testData);
        storageFile.Flush();

        // Verify file was flushed by reading back
        var readData = storageFile.ReadPage(0);
        Assert.Equal(testData, readData.ToArray());
    }

    [Fact]
    public void StorageFile_Dispose_ClosesFile()
    {
        using var storageFile = new StorageFile(_testFilePath, _pageSize, createNew: true);
        storageFile.Dispose();

        // File should be closed and accessible
        Assert.True(File.Exists(_testFilePath));
    }
}