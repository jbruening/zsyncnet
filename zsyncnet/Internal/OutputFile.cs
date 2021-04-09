#define COPY_BLOCKS

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using NLog;

namespace zsyncnet.Internal
{

    public class OutputFile
    {
        public static long AssumedDownloadSpeedBytes { get; set; } = 125000;

        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private enum ChangeType
        {
            Update,
            Remove
        }

        public FileInfo FilePath { get; }
        public FileInfo TempPath { get; }

        public long TotalBytesDownloaded { get; set; }
        private int _blockSize;
        private int _lastBlockSize;
        private long _length;
        private string _sha1;
        private DateTime _mtime;
        private List<BlockSum> _localBlockSums;
        private List<BlockSum> _remoteBlockSums;
        private zsyncnet.ControlFile _cf;

        private readonly int minCopyBlockCount;
        private readonly Action<SyncState> stateUpdate;

        private FileStream _tmpStream;
        private FileStream _existingStream;

        private Uri _fileUri;

        private static HttpClient _client = new HttpClient();


        public OutputFile(FileInfo path, zsyncnet.ControlFile cf, Uri fileUri, Action<SyncState> stateUpdate = null)
        {
            _cf = cf;
            FilePath = path;

            _fileUri = fileUri;
            _blockSize = cf.GetHeader().Blocksize;
            _length = cf.GetHeader().Length;
            _lastBlockSize = (int) (_length % _blockSize == 0 ? _blockSize : _length % _blockSize);
            _sha1 = cf.GetHeader().Sha1;
            _mtime = cf.GetHeader().MTime;
            this.stateUpdate = stateUpdate;

            minCopyBlockCount = (int)(AssumedDownloadSpeedBytes / _blockSize);

            TempPath = new FileInfo(FilePath.FullName + ".part");

            // Create all directories 
            Directory.CreateDirectory(TempPath.Directory.FullName);

            // Open stream

            _tmpStream = new FileStream(TempPath.FullName, FileMode.Create, FileAccess.ReadWrite);
            _existingStream = new FileStream(FilePath.FullName, FileMode.OpenOrCreate, FileAccess.ReadWrite);

            _tmpStream.SetLength(_length);

            _remoteBlockSums = cf.GetBlockSums();
            var fileBuffer = _existingStream.ToByteArray();
            _existingStream.Position = 0;
            _localBlockSums = BlockSum.GenerateBlocksum(fileBuffer,
                cf.GetHeader().WeakChecksumLength, cf.GetHeader().StrongChecksumLength, cf.GetHeader().Blocksize);
            TotalBytesDownloaded = 0;
            // Set the last mod time to the time in the control file. 

        }

        private class DownloadRange
        {
            public long BlockStart, Size;
        }

        private List<DownloadRange> BuildRanges(List<SyncOperation> downloadBlocks)
        {
            // TODO: this is ugly.
            var ranges = new List<DownloadRange>();

            DownloadRange current = null;
            foreach (var downloadBlock in downloadBlocks.Select(block => block.RemoteBlock).OrderBy(block => block.BlockStart))
            {
                if (current == null) // new range
                {
                    current = new DownloadRange
                    {
                        BlockStart = downloadBlock.BlockStart,
                        Size = 1
                    };
                    continue;
                }

                if (downloadBlock.BlockStart == current.BlockStart + current.Size) // append
                {
                    current.Size ++;
                    continue;
                }

                ranges.Add(current);
                current = new DownloadRange
                {
                    BlockStart = downloadBlock.BlockStart,
                    Size = 1
                };
            }
            if (current != null)
                ranges.Add(current);

            return ranges;
        }

        public void Patch()
        {
#if COPY_BLOCKS
            _tmpStream.SetLength(_length);
#else
            _existingStream.CopyTo(_tmpStream);
            _tmpStream.SetLength(_length);
            _existingStream.Close();
#endif

            var syncOps = CompareFiles();
            Logger.Info($"[{_cf.GetHeader().Filename}] Total changed blocks {syncOps.Count}");

            var copyBlocks = syncOps.Where(so => so.LocalBlock != null);
            var downloadBlocks = syncOps.Where(so => so.LocalBlock == null).ToList();

#if COPY_BLOCKS
            //merge copy blocks into download blocks if too small
            var actualCopyBlocks = new List<SyncOperation>();
            {
                var contiguousCopyBlock = new List<SyncOperation>();
                foreach (var copyBlock in copyBlocks.OrderBy(block => block.RemoteBlock.BlockStart))
                {
                    if (contiguousCopyBlock.Count == 0)
                    {
                        contiguousCopyBlock.Add(copyBlock);
                        continue;
                    }

                    if (copyBlock.RemoteBlock.BlockStart == contiguousCopyBlock[0].RemoteBlock.BlockStart + contiguousCopyBlock.Count)
                    {
                        contiguousCopyBlock.Add(copyBlock);
                        continue;
                    }

                    //we've reached the end of a continuous remote copy block
                    if (contiguousCopyBlock.Count > minCopyBlockCount)
                    {
                        //large enough that copying is probably faster than downloading.
                        actualCopyBlocks.AddRange(contiguousCopyBlock);
                    }
                    else
                    {
                        downloadBlocks.AddRange(contiguousCopyBlock);
                    }

                    contiguousCopyBlock.Clear();
                    contiguousCopyBlock.Add(copyBlock);
                }
            }
#else
            foreach (var so in copyBlocks)
            {
                // TODO: handle copy blocks
                // for now, just download them as well
                // TODO: benchmark, find out how important they actually are
                downloadBlocks.Add(so);
            }
#endif

            var downloadRanges = BuildRanges(downloadBlocks);

#if COPY_BLOCKS
            stateUpdate?.Invoke(SyncState.CopyExisting);
            byte[] blockCopy = new byte[_blockSize];
            foreach(var op in actualCopyBlocks)
            {
                long offset = op.RemoteBlock.BlockStart * _blockSize;
                int length = _blockSize;

                if (offset + length > _length) // fix size for last block
                {
                    length = (int)(_length - offset);
                }

                _existingStream.Position = op.LocalBlock.BlockStart * _blockSize;
                _existingStream.Read(blockCopy, 0, length);

                _tmpStream.Position = op.RemoteBlock.BlockStart * _blockSize;
                _tmpStream.Write(blockCopy, 0, length);
            }
            _existingStream.Close();
#endif

            stateUpdate?.Invoke(SyncState.DownloadPatch);

            foreach (var op in downloadRanges)
            {
                long offset = op.BlockStart * _blockSize;
                var length = op.Size * _blockSize;
                var range = new RangeHeaderValue(offset, offset + length - 1);

                var req = new HttpRequestMessage
                {
                    RequestUri = _fileUri,
                    Headers = {Range = range}
                };

                var response = _client.SendAsync(req).Result;
                if (response.IsSuccessStatusCode)
                {

                    Logger.Info($"[{_cf.GetHeader().Filename}] Downloading {range}");
                    var content = response.Content.ReadAsByteArrayAsync().Result;
                    TotalBytesDownloaded += content.Length;
                    if (offset + length > _length) // fix size for last block
                    {
                        length = _length - offset;
                    }

                    _tmpStream.Position = offset;
                    _tmpStream.Write(content, 0, (int)length);
                    _tmpStream.Position = 0;
                }
                else
                {
                    throw new Exception();
                }
            }

            _tmpStream.Flush();
            _tmpStream.Close();
            File.SetLastWriteTimeUtc(TempPath.FullName, _mtime);

        }

        private List<SyncOperation> CompareFiles()
        {
            stateUpdate?.Invoke(SyncState.CalcDiff);

            var syncOps = new List<SyncOperation>();

            for (var i = 0; i < _remoteBlockSums.Count; i++)
            {
                var remoteBlock = _remoteBlockSums[i];

                if (i < _localBlockSums.Count && _localBlockSums[i].ChecksumsMatch(remoteBlock)) continue; // present

                var localBlock = _localBlockSums.Find(x => x.ChecksumsMatch(remoteBlock));

                syncOps.Add(new SyncOperation(remoteBlock, localBlock));
            }

            return syncOps;
        }
    }
}