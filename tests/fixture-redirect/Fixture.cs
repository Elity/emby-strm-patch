// Minimal stand-in with the exact shape patches B and C match on. No Emby code involved.
//
// The shapes here are not decorative: the patcher matches on them (parameter types, the
// Task<StreamHandler> return, the property names it assigns), so a fixture that drifts from
// Emby's real signatures would let a broken patch pass. Verified against stock 4.9.3.0.
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MediaBrowser.Model.Services { public interface IRequest { } }

namespace MediaBrowser.Controller.Net
{
    public class StaticFileResultOptions { public string Path { get; set; } }
}

namespace MediaBrowser.Model.IO
{
    /// <summary>
    /// Emby's handle for "here is the content, and here is what to say about it". Patch C
    /// builds one of these and returns it instead of letting Emby open the file.
    ///
    /// TotalLength is the resource's COMPLETE length and Length is THIS response's body size.
    /// Emby publishes TotalLength as the Content-Range denominator, falling back to
    /// Stream.Length when it is null - which is why the parallel stream refuses to answer
    /// Stream.Length at all.
    /// </summary>
    public class StreamHandler
    {
        public Stream Stream { get; set; }
        public long? TotalLength { get; set; }
        public long? Length { get; set; }
    }
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

        /// <summary>
        /// Patch C's target. Real Emby opens the file here; the stand-in returns a handle whose
        /// stream is a recognisable marker, so a test can tell "Emby's own path ran" from
        /// "the parallel helper ran" without needing Emby.
        /// </summary>
        public Task<MediaBrowser.Model.IO.StreamHandler> GetContent(
            MediaBrowser.Controller.Net.StaticFileResultOptions options,
            long offset, long length, CancellationToken cancellationToken)
        {
            return Task.FromResult(new MediaBrowser.Model.IO.StreamHandler
            {
                Stream = new MemoryStream(System.Text.Encoding.ASCII.GetBytes("HOST-PATH:" + options.Path)),
                TotalLength = null,
                Length = null
            });
        }
    }
}
