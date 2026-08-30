// Minimal stand-in with the exact shape patch A matches on. No Emby code involved.
namespace MediaBrowser.Model.Dto
{
    public class MediaSourceInfo
    {
        public string Path { get; set; }
        public bool SupportsTranscoding { get; set; } = true;
    }
}

namespace Emby.Server.MediaEncoding.Api
{
    public class MediaInfoService
    {
        // Signature only needs to carry a MediaSourceInfo parameter; the patcher finds it by type.
        public void SetDeviceSpecificData(long itemId, string mediaType,
                                          MediaBrowser.Model.Dto.MediaSourceInfo mediaSource,
                                          bool enableDirectPlay, bool enableTranscoding)
        {
            System.GC.KeepAlive(mediaType);
        }
    }
}
