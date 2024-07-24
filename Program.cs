// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using System.Globalization;
using System.Reflection.Metadata;
using System.Runtime.Intrinsics.X86;
using System.Security.Cryptography;
using System.Transactions;
using Couchbase;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.KeyValue;
using Couchbase.Query;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.External;
using Microsoft.Extensions.Logging;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

Console.WriteLine("Hello, World!");

// args are connection string, username, password, number of docs to import, number of chars in body attribute in the doc
// on the cluster create "test" bucket, "test" scope, "test" "test0" "test1" "test2" "test3" collections 
await new StartUsing().Main(args[0], args[1], args[2], args[3], args[4], args[5]);

//await new StartUsing().BulkInsertInTxn();
internal class StartUsing
{
    private static readonly Random random = new();

    public static string RandomString(int length)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[random.Next(s.Length)]).ToArray());
    }

    public async Task Main(string host, string username, string password, string totalS, string sizeS, string expTimeS)
    {
        var total = int.Parse(totalS);
        var expTime = int.Parse(expTimeS);
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

        await ExecuteInTransactionAsync1(username, password, host, total, documento, expTime);

    }


    public async Task<string> Execute(string username, string password, string host, int total, object documento)
    {
        var loggerFactory = LoggerFactory.Create(builder => { builder.AddFilter(l => l >= LogLevel.Information).AddConsole(); });
        var logger = loggerFactory.CreateLogger("ExecuteInTransactionAsync");

        var options = new ClusterOptions().WithCredentials(username, password).WithLogging(loggerFactory);
        var cluster = await Cluster.ConnectAsync(host, options).ConfigureAwait(false);
        var bucket = await cluster.BucketAsync("test");
        var scope = await bucket.ScopeAsync("test");
        var lockCollection = await scope.CollectionAsync("lock");
        
        var lockDocument = new { status = "pending_operation", client = "myclient" };
        for(int i = 0; i <= 10; i++)
        {
            try
            {
                await lockCollection.InsertAsync("lock", lockDocument);
                Console.WriteLine("Lock set");
                break;
            }
            catch (DocumentExistsException)
            {
                Console.WriteLine("Pending operation...");
                var lockResult1 = await lockCollection.GetAsync("lock");
                var lockDocumentPending = lockResult1.ContentAs<dynamic>();
                if (lockDocumentPending.client == "myclient")
                {
                    Console.WriteLine("Retrying previous import of this client"); // lock acquisito da se stesso potrebbe riprovare l'import se i dati dell'import fossero identici
                    break;
                }
                else
                {
                    Thread.Sleep(6000);
                }
            }
        }

        var watch = Stopwatch.StartNew();

        var options1 = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 };


        var _collection = await scope.CollectionAsync("test").ConfigureAwait(false);
        await Parallel.ForEachAsync(Enumerable.Range(0, total), options1, async (index, token) =>
        {
            var result = await _collection.UpsertAsync(index.ToString(), documento,
                    options =>
                    {
                        options.Timeout(TimeSpan.FromSeconds(10));
                    }
                );

            if (index % 100 == 0)
            {
                Console.WriteLine($"Inserted {index:D10} documents - {watch.Elapsed.TotalSeconds:0.00}secs");
            }
        });
        
        var lockResult = await lockCollection.GetAsync("lock");
        var lockDocument1 = lockResult.ContentAs<dynamic>();
        if(lockDocument1.client == "myclient")
        {
            await lockCollection.RemoveAsync("lock");
            Console.WriteLine("Lock removed");
        }
        


        Console.WriteLine($"Total time elapsed - {watch.Elapsed.TotalSeconds:0.00}secs");


        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;
        Console.WriteLine(elapsedMs / 1000 + "s");
        return new string("ciao");

    }






















    public async Task<string> ExecuteInTransactionAsync1(string username, string password, string host, int total, object documento, int expTime)
    {
        var loggerFactory = LoggerFactory.Create(builder => { builder.AddFilter(l => l >= LogLevel.Information).AddConsole(); });
        var logger = loggerFactory.CreateLogger("ExecuteInTransactionAsync");

        var options = new ClusterOptions().WithCredentials(username, password).WithLogging(loggerFactory);
        var cluster = await Cluster.ConnectAsync(host, options).ConfigureAwait(false);
        var bucket = await cluster.BucketAsync("test");
        var metadata_scope = await bucket.ScopeAsync("test");
        var metadata_collection = await metadata_scope.CollectionAsync("test");
        var _transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create()
            .DurabilityLevel(DurabilityLevel.None)
            .ExpirationTime(TimeSpan.FromSeconds(expTime))
            .LoggerFactory(loggerFactory)
            .CleanupLostAttempts(true)
            .CleanupClientAttempts(true)
            .MetadataCollection(metadata_collection)
            .CleanupWindow(TimeSpan.FromSeconds(30))
            .Build());


        var watch = Stopwatch.StartNew();

        var scope = await bucket.ScopeAsync("test").ConfigureAwait(false);


        var tasks = new List<Task>();
        var stopWatch = Stopwatch.StartNew();

        var options1 = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 };

        var _collection = await scope.CollectionAsync("test").ConfigureAwait(false);
        await Parallel.ForEachAsync(Enumerable.Range(0, total), options1, async (index, token) =>
        {
            var result = await _collection.UpsertAsync(index.ToString(), documento,
                    options =>
                    {
                        options.Timeout(TimeSpan.FromSeconds(10));
                    }
                );

            if (index % 100 == 0)
            {
                Console.WriteLine($"Staged {index:D10} documents - {stopWatch.Elapsed.TotalSeconds:0.00}secs");
            }
        });
        
        var stopWatch1 = Stopwatch.StartNew();

        var keys = Enumerable.Range(0, total).Select(n => n.ToString()).ToArray<string>();

        var keysString = "'" + string.Join("', '", keys) + "'";
        var st = "UPSERT INTO testFinal (KEY docId, VALUE doc) SELECT Meta().id as docId, t as doc FROM test as t USE KEYS [" + keysString + "]";
        Console.WriteLine(st);

        try
        {
            await _transactions.QueryAsync<object>(
                st, config => config.ExpirationTime(TimeSpan.FromSeconds(expTime)));
            
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





        Console.WriteLine($"Total transaction time elapsed - {stopWatch1.Elapsed.TotalSeconds:0.00}secs");


        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;
        Console.WriteLine(elapsedMs / 1000 + "s");
        return new string("ciao");

    }





public async Task<string> ExecuteInTransactionAsync(string username, string password, string host, int total, object documento, int queryChunk, int expTime)
    {
        var loggerFactory = LoggerFactory.Create(builder => { builder.AddFilter(l => l >= LogLevel.Information).AddConsole(); });
        var logger = loggerFactory.CreateLogger("ExecuteInTransactionAsync");

        var options = new ClusterOptions().WithCredentials(username, password).WithLogging(loggerFactory);
        var cluster = await Cluster.ConnectAsync(host, options).ConfigureAwait(false);
        var bucket = await cluster.BucketAsync("test");
        var metadata_scope = await bucket.ScopeAsync("test");
        var metadata_collection = await metadata_scope.CollectionAsync("test");
        var _transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create()
            .DurabilityLevel(DurabilityLevel.None)
            .ExpirationTime(TimeSpan.FromSeconds(expTime))
            .LoggerFactory(loggerFactory)
            .CleanupLostAttempts(true)
            .CleanupClientAttempts(true)
            .MetadataCollection(metadata_collection)
            .CleanupWindow(TimeSpan.FromSeconds(30))
            .Build());


        var watch = Stopwatch.StartNew();

        var scope = await bucket.ScopeAsync("test").ConfigureAwait(false);


        var tasks = new List<Task>();
        var stopWatch = Stopwatch.StartNew();

        var options1 = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 };
        //await Parallel.ForEachAsync(Enumerable.Range(0, total), options1, async (i, token1) =>
        //{
        for (var i = 0; i <= 0; i++)
        {
                var _collection = await scope.CollectionAsync("test" + i).ConfigureAwait(false);
                await Parallel.ForEachAsync(Enumerable.Range(0, total), options1, async (index, token) =>
                // for (int index = 0; index < total; index ++)
                    {
                        var result = await _collection.UpsertAsync(index.ToString() + "-" + i, documento,
                                options =>
                                {
                                    options.Timeout(TimeSpan.FromSeconds(10));
                                }
                            );

                        if (index % 100 == 0)
                        {
                            Console.WriteLine($"Collection {i}, Staged {index:D10} documents - {stopWatch.Elapsed.TotalSeconds:0.00}secs");
                        }
                    });
         }
        var stopWatch1 = Stopwatch.StartNew();
        try
        {
           
            var transactionResult = await _transactions.RunAsync(async ctx =>
            {

                for (var i = 0; i <= 0; i++)
                {
                    var keys = Enumerable.Range(0, total).Select(n => n.ToString() + "-" + i).ToArray<string>();
                    var numChunks = total / queryChunk;
                    for (int j = 0; j <= numChunks && (queryChunk * j < total); j++)
                    {
                        var keysString = "'" + string.Join("', '", keys.Skip(queryChunk * j).Take(queryChunk)) + "'";
                        var st = "UPSERT INTO testFinal (KEY docId, VALUE doc) SELECT Meta().id as docId, t as doc FROM test" + i + " as t USE KEYS [" + keysString + "]";
                        Console.WriteLine(st);
                        Console.WriteLine($"Transaction time elapsed - {stopWatch1.Elapsed.TotalSeconds:0.00}secs");
                        Console.WriteLine($"Total time elapsed - {stopWatch.Elapsed.TotalSeconds:0.00}secs");
 
                        IQueryResult<object> qr = await ctx.QueryAsync<object>(st,
                         
                            scope: scope);
      
                        //  await bucket.WaitUntilReadyAsync(TimeSpan.FromSeconds(10));

                    }
                    
                }
                await ctx.CommitAsync();
            });
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

            

    

        Console.WriteLine($"Total transaction time elapsed - {stopWatch1.Elapsed.TotalSeconds:0.00}secs");


        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;
        Console.WriteLine(elapsedMs / 1000 + "s");
        return new string("ciao");

    }

}