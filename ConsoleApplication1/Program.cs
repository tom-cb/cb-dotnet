using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Diagnostics;

using Couchbase;
using Couchbase.Configuration.Client;



// A Hello World! program in C#. 

internal class Program
{
    
    static int DOC_COUNT = 10000;
    static bool OUTPUT_READS = false;

    private static void Main(string[] args)
    {

        var config = getClientConfig();

        Cluster Cluster = new Cluster(config);

        using (var bucket = Cluster.OpenBucket())
        {
            var watch = Stopwatch.StartNew();

            //createDocs(bucket);

            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            Console.WriteLine("time: " + elapsedMs + "ms");


            List<String> keys = new List<string>();
            for (int i = 0; i <= DOC_COUNT; i++)
            {
                keys.Add(i.ToString());
            }

            getBulkDocs(bucket, keys);

            getBulkDocsAsync(bucket, keys);

            Console.Read();

        }
    }

    private async static void getBulkDocsAsync(Couchbase.Core.IBucket bucket, List<string> keys)
    {
        var watch = Stopwatch.StartNew();

        List<Task<IDocumentResult<dynamic>>> getTasks = new List<Task<IDocumentResult<dynamic>>>();

        foreach (var k in keys) {
            Task<IDocumentResult<dynamic>> getTask = bucket.GetDocumentAsync<dynamic>(k);
            getTasks.Add(getTask);
        }

        IDocumentResult<dynamic>[] results = await Task.WhenAll(getTasks);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;

        Console.WriteLine("Async Read time: " + elapsedMs + "ms");

        foreach (var res in results)
        {
            //Console.WriteLine(res.Status + " " + res.Content);
        }


        //Console.WriteLine("has value: \n" + result.Value);

    }

    private static void getBulkDocs(Couchbase.Core.IBucket bucket, List<string> keys)
    {

        var watch = Stopwatch.StartNew();

        var multiGet = bucket.Get<dynamic>(keys, new ParallelOptions
        {
            MaxDegreeOfParallelism = 4
        }, 4);

        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;

        Console.WriteLine("Read time: " + elapsedMs + "ms");

        if (OUTPUT_READS)
        {
            foreach (var item in multiGet)
            {
                if (!item.Value.Success)
                {
                    Console.WriteLine("Failed to read key: " + item.Value.Message);
                }
                else
                {
                    Console.WriteLine(item.Key + " has value: \n" + item.Value.Value);
                }
            }
        }
    }

    private static void createDocs(Couchbase.Core.IBucket bucket)
    {
        for (int i = 0; i <= DOC_COUNT; i++)
        {
            var document = new Document<dynamic>
            {
                Id = i.ToString(),
                Content = new
                {
                    name = "Couchbase test application"
                }
            };

            //Console.WriteLine(document.Id);

            var upsert = bucket.Upsert(document);

           // Console.WriteLine(upsert.Status);

            if (upsert.Success)
            {
                var get = bucket.GetDocument<dynamic>(i.ToString());
                document = get.Document;
                //var msg = string.Format("{0} {1}!", document.Id, document.Content.name);
                //Console.WriteLine(msg);
            }
        }

    }

    private static ClientConfiguration getClientConfig()
    {
        var config = new ClientConfiguration
        {
            Servers = new List<Uri> {
                 new Uri("http://127.0.0.1:8091/pools")
            },
            UseSsl = false,
            DefaultOperationLifespan = 1000,
            BucketConfigs = new Dictionary<string, BucketConfiguration>
            {
             {"default", new BucketConfiguration
            {
                BucketName = "default",
                UseSsl = false,
                Password = "",
                DefaultOperationLifespan = 2000,
                PoolConfiguration = new PoolConfiguration
                {
                    MaxSize = 10,
                    MinSize = 5,
                    SendTimeout = 12000
                }
            }}
            }
        };

        return config;
    }
}

