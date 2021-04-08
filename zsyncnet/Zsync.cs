using System;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using Flurl.Http;
using zsyncnet.Internal;

namespace zsyncnet
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class Zsync
    {
        private static bool IsAbsoluteUrl(string url)
        {
            Uri result;
            return Uri.TryCreate(url, UriKind.Absolute, out result);
        }
        
        /// <summary>
        /// Syncs a file
        /// </summary>
        /// <param name="zsyncFile"></param>
        /// <param name="output"></param>
        /// <returns>Number of bytes downloaded</returns>
        /// <exception cref="WebException"></exception>
        /// <exception cref="Exception"></exception>
        public static long Sync(Uri zsyncFile, DirectoryInfo output)
        {
            // Load zsync control file            
            var cf = new ControlFile(zsyncFile.ToString().GetStreamAsync().Result);
            var outputFile = new FileInfo(Path.Combine(output.FullName, cf.GetHeader().Filename.TrimStart()));

            return Sync(cf, zsyncFile, outputFile);            
        }


        /// <summary>
        /// Syncs a file
        /// </summary>
        /// <param name="zsyncFile"></param>
        /// <param name="outputFile"></param>
        /// <param name="fileUri"></param>
        /// <returns>Number of bytes downloaded</returns>
        /// <exception cref="WebException"></exception>
        /// <exception cref="Exception"></exception>
        public static long Sync(Uri zsyncFile, FileInfo outputFile, Uri fileUri = null)
        {
            // Load zsync control file
            var cf = new ControlFile(zsyncFile.ToString().GetStreamAsync().Result);
            return Sync(cf, zsyncFile, outputFile, fileUri);
        }

        private static long Sync(ControlFile cf, Uri zsyncFile, FileInfo outputFile, Uri fileUri = null)
        {

            if (fileUri == null)
            {
                if (cf.GetHeader().Url == null || !IsAbsoluteUrl(cf.GetHeader().Url))
                {
                    // Relative
                    fileUri = new Uri(zsyncFile.ToString().Replace(".zsync", string.Empty));
                }
                else
                {
                    fileUri = new Uri(cf.GetHeader().Url);
                }
            }

            if (fileUri.ToString().HeadAsync().Result.StatusCode == HttpStatusCode.NotFound)
            {
                // File not found 
                throw new WebException("File not found");
            }

            if (outputFile.Exists)
            {
                // File exists, use the existing file as the seed file 

                OutputFile of = new OutputFile(outputFile, cf, fileUri);

                of.Patch();

                if (VerifyFile(of.TempPath, cf.GetHeader().Sha1))
                {
                    File.Copy(of.TempPath.FullName, of.FilePath.FullName, true);
                    File.Delete(of.TempPath.FullName);
                }
                else
                {
                    throw new Exception("File invalid");
                }

                return of.TotalBytesDownloaded;
            }
            else
            {
                fileUri.ToString().DownloadFileAsync(outputFile.Directory.FullName, outputFile.Name).Wait();
                return cf.GetHeader().Length;

            }
        }

        private static bool VerifyFile(FileInfo file, string checksum)
        {
            using (SHA1CryptoServiceProvider crypto = new SHA1CryptoServiceProvider())
            {
                var buffer = File.ReadAllBytes(file.FullName);
                var hash = ZsyncUtil.ByteToHex(crypto.ComputeHash(buffer));

                return hash == checksum;
            }
        }

    }
}