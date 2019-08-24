using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;
using System.Xml.Linq;
using DotNet.Collections.Generic;
using NLog;

namespace zsyncnet.Internal
{

    public class OutputFile
    {
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

        private FileStream _tmpStream;
        private FileStream _existingStream;

        private Uri _fileUri;

        private static HttpClient _client = new HttpClient();


        public OutputFile(FileInfo path, zsyncnet.ControlFile cf, Uri fileUri)
        {
            _cf = cf;
            FilePath = path;

            _fileUri = fileUri;
            _blockSize = cf.GetHeader().Blocksize;
            _length = cf.GetHeader().Length;
            _lastBlockSize = (int) (_length % _blockSize == 0 ? _blockSize : _length % _blockSize);
            _sha1 = cf.GetHeader().Sha1;
            _mtime = cf.GetHeader().MTime;

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

        public void Patch()
        {

            _existingStream.CopyTo(_tmpStream);

            _tmpStream.SetLength(_length);

            _existingStream.Close();

            // Fetch and Patch

            //var delta = BuildDelta();

            var syncOps = CompareFiles();

            Logger.Info($"[{_cf.GetHeader().Filename}] Total changed blocks {syncOps.Count}");
            int count = 0;
            foreach (var op in syncOps)
            {
                if (op.LocalBlock != null)
                {
                    // TODO: implement
                    // for now: just fetch it
                    op.LocalBlock = null;
                }
                //Console.WriteLine(op.LocalBlock);
                // If the local block is null, we need to acquire
                if (op.LocalBlock == null)
                {
                    var range = GetRange(op.RemoteBlock.BlockStart);

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
                        var offset = op.RemoteBlock.BlockStart * _blockSize;
                        var length = _blockSize;
                        if (offset + _blockSize > _length)
                        {
                            length = _lastBlockSize;
                        }

                        _tmpStream.Position = offset;
                        _tmpStream.Write(content, 0, length);
                        _tmpStream.Position = 0;
                    }
                    else
                    {
                        throw new Exception();
                    }

                    /*
                    foreach (var x in delta)
                    {
                        if (x.Value != ChangeType.Update) continue;
                        var range = GetRange(x.Key);
        
                        Console.WriteLine(range.ToString());
                        var req = new HttpRequestMessage
                        {
                            RequestUri = _fileUri,
                            Headers = {Range = range}
                        };
        
                        var response = _client.SendAsync(req).Result;
                        if (response.IsSuccessStatusCode)
                        {
                            var content = response.Content.ReadAsByteArrayAsync().Result;
                            TotalBytesDownloaded += content.Length;
                            var offset = x.Key * _blockSize;
                            var length = _blockSize;
                            if (offset + _blockSize > _length)
                            {
                                length = _lastBlockSize;
                            }
        
                            _tmpStream.Position = offset;
                            _tmpStream.Write(content, 0, length);
                        }
        
                    }
                    */
                }
            }

            _tmpStream.Flush();
            _tmpStream.Close();
            File.SetLastWriteTimeUtc(TempPath.FullName, _mtime);

        }

        private RangeHeaderValue GetRange(int block)
        {
            long offset = block * _blockSize;
            return new RangeHeaderValue(offset, offset + _blockSize - 1);
        }

        private List<SyncOperation> CompareFiles()
        {
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