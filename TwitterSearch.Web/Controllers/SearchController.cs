using Legacy.Common.Models;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace TwitterSearch.Web.Controllers
{
  public class SearchController : ApiController
  {
    [HttpPost]
    [Route("search")]
    public void HandleNotification([FromBody] SearchRequest request)
    {
      var connString = ConfigurationManager.ConnectionStrings["StorageConnectionString"].ConnectionString;
      var storageAccount = CloudStorageAccount.Parse(connString);

      var queueClient = storageAccount.CreateCloudQueueClient();
      var queue = queueClient.GetQueueReference("search-queue");
      queue.CreateIfNotExists();

      var serialized = JsonConvert.SerializeObject(request);
      var message = new CloudQueueMessage(serialized);
      queue.AddMessage(message);
    }
  }
}
