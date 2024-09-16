﻿// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using System.Runtime.InteropServices;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.External;
using Microsoft.Extensions.Logging;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

Console.WriteLine("Hello, World!");

// args are connection string, username, password, number of docs to import, number of chars in body attribute in the doc
// on the cluster create "test" bucket, "test" scope, "test" "test0" "test1" "test2" "test3" collections 
await new StartUsing().Main(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);

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

    public async Task Main(string host, string username, string password, string totalS, string sizeS, string useQuery, string queryExpireTime)
    {
        var total = int.Parse(totalS);


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

        if (int.Parse(useQuery) == 0)
        {
            await ExecuteInKeyValueTransactionAsync(username, password, host, total, documento);
        }

        if (int.Parse(useQuery) == 1)
        {
            await ExecuteInQueryTransactionAsync(username, password, host, total, documento, int.Parse(queryExpireTime));
        }

    }

    public async Task updateDocs(string i, ICluster cluster, AttemptContext ctx, object documento, int total)
    {
        var bucket = await cluster.BucketAsync("test").ConfigureAwait(false);
        var scope = await bucket.ScopeAsync("test").ConfigureAwait(false);
        var _collection = await scope.CollectionAsync("test" + i).ConfigureAwait(false);

        var tasks = new List<Task>();
        var stopWatch = Stopwatch.StartNew();
        var options1 = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount  };
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
                Console.WriteLine($"Collection {i}, Staged {index:D10} documents - {stopWatch.Elapsed.TotalSeconds:0.00}secs");
            }
        });

        await Task.WhenAll(tasks.ToArray());
    }

    public async Task<string> ExecuteInKeyValueTransactionAsync(string username, string password, string host, int total, object documento)
    {
        var loggerFactory = LoggerFactory.Create(builder => { builder.AddFilter(l => l > LogLevel.Information).AddConsole(); });
        var logger = loggerFactory.CreateLogger("ExecuteInTransactionAsync");

        var options = new ClusterOptions() { NumKvConnections = 128 }.WithCredentials(username, password).WithLogging(loggerFactory);
        var cluster = await Cluster.ConnectAsync(host, options).ConfigureAwait(false);
        var bucket = await cluster.BucketAsync("test");
        var metadata_scope = await bucket.ScopeAsync("test");
        var metadata_collection = await metadata_scope.CollectionAsync("test");
        var _transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create()
            .DurabilityLevel(DurabilityLevel.Majority)
            .ExpirationTime(TimeSpan.FromSeconds(120))
            .LoggerFactory(loggerFactory)
            .CleanupLostAttempts(true)
            .CleanupClientAttempts(true)
            .MetadataCollection(metadata_collection)
            .CleanupWindow(TimeSpan.FromSeconds(1))
            .Build());


        var watch = Stopwatch.StartNew();

        var result = await _transactions.RunAsync(async ctx =>
        {
            for (var i = 0; i <= 3; i++)
            {
                while (true)
                {
                    try
                    {
                        await updateDocs(i.ToString(), cluster, ctx, documento, total);
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
            }
        });

        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;
        Console.Clear();
        Console.WriteLine(elapsedMs / 1000 + "s");
        return new string("");

    }


    public async Task<string> ExecuteInQueryTransactionAsync(string username, string password, string host, int total, object documento, int expTime)
    {
        var loggerFactory = LoggerFactory.Create(builder => { builder.AddFilter(l => l >= LogLevel.Information).AddConsole(); });
        var logger = loggerFactory.CreateLogger("ExecuteInTransactionAsync");

        var options = new ClusterOptions() { QueryTimeout = TimeSpan.FromSeconds(expTime) }.WithCredentials(username, password).WithLogging(loggerFactory);
        var cluster = await Cluster.ConnectAsync(host, options);
        var bucket = await cluster.BucketAsync("test");

        var _transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create()
            .ExpirationTime(TimeSpan.FromSeconds(expTime))
            .Build());


        var watch = Stopwatch.StartNew();

        var scope = await bucket.ScopeAsync("test");


        var tasks = new List<Task>();
        var stopWatch = Stopwatch.StartNew();

        var options1 = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 8 };

        var _collection = await scope.CollectionAsync("test");

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
                Console.WriteLine($"Upserted {index:D10} documents - {stopWatch.Elapsed.TotalSeconds:0.00}secs");
            }
        });

        var stopWatch1 = Stopwatch.StartNew();

        var st = "UPSERT INTO test.test.testFinal (KEY docId, VALUE doc) SELECT Meta().id as docId, t as doc FROM test.test.test as t USE KEYS (SELECT RAW TO_STRING(_keys) FROM ARRAY_RANGE(0, " + total + ") AS _keys)";

        try
        {

            await cluster.QueryAsync<object>(st, options => options
            .Raw("tximplicit", true)
            .Raw("txtimeout", expTime + "s")
            .Raw("durability_level", "none")
            .Raw("timeout", expTime + "s")
            .Raw("kvtimeout", expTime + "s"));

        }
        catch (Exception e)
        {
            logger.LogError("Transaction operation failed " + e.Message);
        }


        Console.WriteLine($"Total transaction time elapsed - {stopWatch1.Elapsed.TotalSeconds:0.00}secs");

        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;
        Console.WriteLine(elapsedMs / 1000 + "s");
        return new string("");

    }

}