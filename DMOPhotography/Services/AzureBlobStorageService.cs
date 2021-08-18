using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;

namespace DMOPhotography.Services
{
    class AzureBlobStorageService
    {
        readonly CloudBlobClient _client;

        public AzureBlobStorageService(CloudBlobClient client) => _client = client;

        public async Task<CloudBlockBlob> SaveBlockBlob(string containerName, Stream photoStream, string blobTitle)
        {
            var blobContainer = GetBlobContainer(containerName);

            var blockBlob = blobContainer.GetBlockBlobReference(blobTitle);
            await blockBlob.UploadFromStreamAsync(photoStream).ConfigureAwait(false);

            return blockBlob;
        }

        public async IAsyncEnumerable<string> GetContainerNames([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            BlobContinuationToken? continuationToken = null;

            do
            {
                var response = await _client.ListContainersSegmentedAsync(continuationToken, cancellationToken).ConfigureAwait(false);
                continuationToken = response.ContinuationToken;

                foreach (var container in response?.Results ?? Enumerable.Empty<CloudBlobContainer>())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    yield return container.Name;
                }

            } while (continuationToken != null);
        }

        public async IAsyncEnumerable<T> GetBlobs<T>(string containerName, [EnumeratorCancellation] CancellationToken cancellationToken, string prefix = "", int? maxresultsPerQuery = null, BlobListingDetails blobListingDetails = BlobListingDetails.None) where T : ICloudBlob
        {
            var blobContainer = GetBlobContainer(containerName);

            BlobContinuationToken? continuationToken = null;

            do
            {
                var response = await blobContainer.ListBlobsSegmentedAsync(prefix, true, blobListingDetails, maxresultsPerQuery, continuationToken, null, null, cancellationToken).ConfigureAwait(false);
                continuationToken = response?.ContinuationToken;

                foreach (var blob in response?.Results?.OfType<T>() ?? Enumerable.Empty<T>())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    yield return blob;
                }

            } while (continuationToken != null);
        }

        CloudBlobContainer GetBlobContainer(string containerName) => _client.GetContainerReference(containerName);
    }
}
