// Minimal stand-in with the exact shape patch B matches on. No Emby code involved.
using System.Threading.Tasks;

namespace MediaBrowser.Model.Services { public interface IRequest { } }

namespace MediaBrowser.Controller.Net
{
    public class StaticFileResultOptions { public string Path { get; set; } }
}

namespace Emby.Server.Implementations.HttpServer
{
    public class HttpResultFactory
    {
        public object GetRedirectResult(string url) => "REDIRECT:" + url;

        public Task<object> GetStaticFileResult(
            MediaBrowser.Model.Services.IRequest requestContext,
            MediaBrowser.Controller.Net.StaticFileResultOptions options)
            => Task.FromResult<object>("STREAM:" + options.Path);
    }
}
