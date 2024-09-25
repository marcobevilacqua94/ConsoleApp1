// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.External;
using Microsoft.Extensions.Logging;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

await new StartUsing().Main(args[0], args[1], args[2], args[3], args[4], args[5]);

internal class StartUsing
{
    private static readonly Random random = new();

    public static string RandomString(int length)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[random.Next(s.Length)]).ToArray());
    }

    public async Task Main(string host, string username, string password, string totalS, string sizeS, string expiryTimeS)
    {
        var total = int.Parse(totalS);
        var expiryTime = int.Parse(expiryTimeS);

        var documento = new
        {
            _id = "667bfaddb0463c180d804cc9",
            index = 0,
            guid = "5225fdb9-b9c4-4887-8220-7a5eca255531",
            isActive = true,
            balance = "$1,022.47",
            picture = "http=//placehold.it/32x32",
            age = 39,
            body = RandomString(int.Parse(sizeS)),
            eyeColor = "brown",
            name = "Sherri Burke",
            gender = "female",
            company = "ZILLANET",
            email = "sherriburke@zillanet.com"
        };


        await ExecuteInKeyValueTransactionAsync(username, password, host, total, documento, expiryTime);
        

    }

    public async Task updateDocs(ICluster cluster, AttemptContext ctx, object documento, int total)
    {
        var bucket = await cluster.BucketAsync("test").ConfigureAwait(false);
        var scope = await bucket.ScopeAsync("test").ConfigureAwait(false);
        var _collection = await scope.CollectionAsync("test").ConfigureAwait(false);

        var tasks = new List<Task>();
        var stopWatch = Stopwatch.StartNew();
        var options1 = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount };
        await Parallel.ForEachAsync(Enumerable.Range(0, total), options1, async (index, token) =>
        {

            var opt = await ctx.GetOptionalAsync(_collection, index.ToString());
            if (opt == default)
            {
                tasks.Add(ctx.ReplaceAsync(opt, documento));
            }
            else
            {
                tasks.Add(ctx.InsertAsync(_collection, index.ToString(), documento));
            }

            if (index % 100 == 0)
            {
                Console.WriteLine($"Staged {index:D10} documents - {stopWatch.Elapsed.TotalSeconds:0.00}secs");
            }
        });
    }

    public async Task<string> ExecuteInKeyValueTransactionAsync(string username, string password, string host, int total, object documento, int expiryTime)
    {
        var loggerFactory = LoggerFactory.Create(builder => { builder.AddFilter(l => l > LogLevel.Information).AddConsole(); });
        var logger = loggerFactory.CreateLogger("ExecuteInTransactionAsync");

        var options = new ClusterOptions() { NumKvConnections = 128 }.WithCredentials(username, password).WithLogging(loggerFactory);
        var cluster = await Cluster.ConnectAsync(host, options).ConfigureAwait(false);
        var bucket = await cluster.BucketAsync("test");

        var metadata_scope = await bucket.ScopeAsync("test");
        var metadata_collection = await metadata_scope.CollectionAsync("test");
        var _transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create()
            .ExpirationTime(TimeSpan.FromSeconds(expiryTime))
            .LoggerFactory(loggerFactory)
            .CleanupLostAttempts(true)
            .CleanupClientAttempts(true)
            .MetadataCollection(metadata_collection)
            .CleanupWindow(TimeSpan.FromSeconds(20))
            .Build());


        var watch = Stopwatch.StartNew();

        var result = await _transactions.RunAsync(async ctx =>
        {

                while (true)
                {
                    try
                    {
                        await updateDocs(cluster, ctx, documento, total);
                        break;
                    }
                    catch (TransactionOperationFailedException e)
                    {
                        logger.LogError("Transaction operation failed " + e.Message);
                    }
                    catch (TransactionCommitAmbiguousException e)
                    {
                        logger.LogError("Transaction possibly committed " + e.Message);
                    }
                    catch (TransactionExpiredException e)
                    {
                        logger.LogError("Transaction expired, trying again " + e.Message);
                    }
                    catch (TransactionFailedException e)
                    {
                        logger.LogError("Transaction did not reach commit point " + e.Message);
                    }
                }
           
        });

        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;
        Console.Clear();
        Console.WriteLine(elapsedMs / 1000 + "s");
        return new string("");

    }

}