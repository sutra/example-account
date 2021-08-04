package org.oxerr.example.account.service;

import org.oxerr.example.account.Account;

public interface AccountService {

	Account get(final long id);

	Account addAmount(final long id, final long amount);

}
