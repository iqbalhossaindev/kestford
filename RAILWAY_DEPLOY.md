# Railway deploy notes

1. Put the extracted contents of this folder at the root of your GitHub repository.
2. Deploy the repository to Railway.
3. Add two volumes:
   - `/app/Playlist`
   - `/app/data`
4. Generate a public domain for the service.
5. If Railway asks for a target port, use `10000`.

This package seeds an empty mounted `/app/Playlist` volume from `/app/defaults/Playlist` on first boot.
