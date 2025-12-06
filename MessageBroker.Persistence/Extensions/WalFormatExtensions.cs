using System.Globalization;

namespace MessageBroker.Persistence.Extensions;

public static class WalFormatExtensions
{
    private const string TimestampFormat = "yyyyMMddHHmmss";
    
    extension(IEnumerable<string> files)
    {
        public IEnumerable<string> OrderByWalFormat()
        {
            return files
                .Select(ParseWalFileInfo)
                .Where(f => f is not null)
                .OrderBy(f => f!.CreationTime)
                .ThenBy(f => f!.SegmentNumber)
                .Select(f => f!.OriginalPath);
        }
    }
    
    private static WalFileParts? ParseWalFileInfo(string filePath)
    {
        try
        {
            string fileName = Path.GetFileNameWithoutExtension(filePath);
            
            string[] parts = fileName.Split('-');

            if (parts.Length < 3)
            {
                return null;
            }
            
            string segmentPart = parts[^1];
            string timePart = parts[^2];

            if (!int.TryParse(segmentPart, out int segmentNumber))
            {
                return null;
            }
            
            if (!DateTimeOffset.TryParseExact(timePart, TimestampFormat, 
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out var creationTime))
            {
                return null;
            }

            return new WalFileParts(filePath, creationTime, segmentNumber);
        }
        catch
        {
            return null;
        }
    }
    
    private record WalFileParts(
        string OriginalPath,
        DateTimeOffset CreationTime,
        int SegmentNumber);
}