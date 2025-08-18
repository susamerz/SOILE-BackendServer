package fi.abo.kogni.soile2.http_server.userManagement;

import java.util.Map;

import io.vertx.ext.auth.hashing.HashingAlgorithm;
import io.vertx.ext.auth.hashing.HashingStrategy;

/**
 * This is a simple Hashing Strategy, that adds a Server Salt to the individual salts for each password.
 * We will assume, that we will always obtain a hashed password, and we only ever handle hashed passwords. 
 * @author Thomas Pfau
 *
 */
public class SoileHashing implements HashingStrategy {

	HashingStrategy vertxDefault = HashingStrategy.load();
	private String serverSalt;
	/**
	 * Default constructor setting the server salt.
	 * @param serverSalt the Server salt to use.
	 */
	public SoileHashing(String serverSalt) {
		this.serverSalt = serverSalt;
	}
	
	private String prefix(String input)
	{
		return "S" + input;
	}
	
	private String dePrefix(String prefixedString)
	{
		return prefixedString.substring(1);
	}
	
	@Override
	public String hash(String id, Map<String, String> params, String salt, String password) {
		password = password + serverSalt;
		return prefix(vertxDefault.hash(id, params, salt, password));
	}

	@Override
	public boolean verify(String hash, String password) {		
		return vertxDefault.verify(dePrefix(hash), password+serverSalt);
	}

	@Override
	public HashingAlgorithm get(String id) {
		return vertxDefault.get(id);
	}

	@Override
	public HashingStrategy put(String id, HashingAlgorithm algorithm) {
		return vertxDefault.put(id, algorithm);
	}

}
