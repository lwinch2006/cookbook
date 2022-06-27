using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace Cookbook.ASPNET.Authorization.OAuth;

public static class OAuthExtensions
{
	private const string TenantIdSettingName = "TenantId";
	private const string ClientIdSettingName = "ClientId";
	private const string ClientSecretSettingName = "ClientSecret";
	private const string RedirectUrlSettingName = "RedirectUrl";

	public static async Task<string> GetMicrosoftAccessToken(IConfiguration configuration)
	{
		var authenticationContext = new AuthenticationContext($"https://login.microsoftonline.com/{configuration[TenantIdSettingName]}");
		var authenticationResult = await authenticationContext.AcquireTokenAsync(
			"https://management.azure.com/",
			new ClientCredential(
				configuration[ClientIdSettingName], 
				configuration[ClientSecretSettingName]));

		if (authenticationResult == null) {
			throw new InvalidOperationException("Failed to obtain the JWT token");
		}

		return authenticationResult.AccessToken;
	}
}