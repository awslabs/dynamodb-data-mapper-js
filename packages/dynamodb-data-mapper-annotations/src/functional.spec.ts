import {attribute} from "./attribute";
import {table} from "./table";
import {DataMapper, DynamoDbSchema} from '@aws/dynamodb-data-mapper';
import {isSchema} from '@aws/dynamodb-data-marshaller';

export class Author {
    @attribute()
    name: string;

    @attribute({memberType: {type: 'String'}})
    socialMediaHandles: Map<string, string>;

    @attribute()
    photo: Uint8Array;
}

export class Comment {
    /**
     * The time at which this comment was posted
     */
    @attribute()
    timestamp?: Date;

    /**
     * Whether this comment has been approved by a moderator.
     */
    @attribute()
    approved?: boolean;

    /**
     * The title of the comment
     */
    @attribute()
    subject?: string;

    /**
     * The text of the comment
     */
    @attribute()
    text?: string;

    /**
     * The handle of the comment author
     */
    @attribute()
    author?: string;

    /**
     * The number of upvotes this comment has received.
     */
    @attribute()
    upvotes?: number;

    /**
     * The number of downvotes this comment has received.
     */
    @attribute()
    downvotes?: number;

    /**
     * Replies to this comment
     */
    @attribute({
        memberType: {
            type: 'Document',
            members: (Comment.prototype as any)[DynamoDbSchema],
            valueConstructor: Comment
        }
    })
    replies?: Array<Comment>;
}

@table('Posts')
export class Post {
    @attribute({keyType: 'HASH'})
    id: string;

    @attribute()
    author?: Author;

    @attribute()
    content?: string;

    @attribute()
    title?: string;

    @attribute()
    subtitle?: string;

    @attribute()
    imageLink?: string;

    @attribute({memberType: {type: 'String'}})
    corrections?: Array<string>;

    /**
     * Replies to this post
     */
    @attribute({
        memberType: {
            type: 'Document',
            members: (Comment.prototype as any)[DynamoDbSchema],
            valueConstructor: Comment
        }
    })
    replies?: Array<Comment>;

    @attribute({memberType: 'String'})
    tags: Set<string>;
}

describe('annotations', () => {
    it('should create a schema that includes references to property schemas', () => {
        const postSchema = (Post.prototype as any)[DynamoDbSchema];
        expect(isSchema(postSchema)).toBe(true);
        expect(isSchema(postSchema.author.members)).toBe(true);
        expect(isSchema(postSchema.replies.memberType.members)).toBe(true);
    });

    it('should support recursive shapes in the generated schema', () => {
        const commentSchema = (Comment.prototype as any)[DynamoDbSchema];
        expect(isSchema(commentSchema)).toBe(true);
        expect(isSchema(commentSchema.replies.memberType.members)).toBe(true);
        expect(commentSchema.replies.memberType.members).toBe(commentSchema);
    });

    it('should marshall a full object graph according to the schema', async () => {
        const promiseFunc = jest.fn(() => Promise.resolve({Item: {}}));
        const mockDynamoDbClient = {
            putItem: jest.fn(() => ({promise: promiseFunc})),
        };

        const mapper = new DataMapper({
            client: mockDynamoDbClient as any,
        });

        const post = new Post();
        post.id = 'postId';
        post.author = new Author();
        post.author.name = 'John Smith';
        post.author.photo = Uint8Array.from([0xde, 0xad, 0xbe, 0xef]);
        post.author.socialMediaHandles = new Map([
            ['github', 'john_smith_27834231'],
            ['twitter', 'theRealJohnSmith'],
        ]);
        post.title = 'Review of Rob Loblaw\'s Law Blog';
        post.subtitle = 'Does it live up to the hype?';
        post.content = "It's a great law blog.";
        post.corrections = [
            'The first edition of this post did not adequately attest to the law blog\'s greatness.'
        ];
        post.replies = [new Comment()];

        post.replies[0].author = 'Rob Loblaw';
        post.replies[0].timestamp = new Date(0);
        post.replies[0].subject = 'Great review';
        post.replies[0].text = 'Appreciate the congrats';
        post.replies[0].upvotes = 35;
        post.replies[0].downvotes = 0;
        post.replies[0].approved = true;

        const reply = new Comment();
        reply.author = 'John Smith';
        reply.timestamp = new Date(60000);
        reply.subject = 'Great review of my review';
        reply.text = 'Thanks for reading!';
        reply.approved = true;

        post.replies[0].replies = [reply];

        await mapper.put({item: post});

        expect(mockDynamoDbClient.putItem.mock.calls[0][0])
            .toMatchObject({
                TableName: 'Posts',
                Item: {
                    author: {M: {
                        name: {S: "John Smith"},
                        photo: {B: Uint8Array.from([0xde, 0xad, 0xbe, 0xef])},
                        socialMediaHandles: {M: {
                            github: {S: "john_smith_27834231"},
                            twitter: {S: "theRealJohnSmith"}
                        }}
                    }},
                    content: {S: "It's a great law blog."},
                    corrections: {L: [
                        {S: "The first edition of this post did not adequately attest to the law blog's greatness."}
                    ]},
                    id: {S: "postId"},
                    replies: {L: [
                        {M: {
                            approved: {BOOL: true},
                            author: {S: "Rob Loblaw"},
                            downvotes: {N: "0"},
                            replies: {L: [
                                {M: {
                                    approved: {BOOL: true},
                                    author: {S: "John Smith"},
                                    subject: {S: "Great review of my review"},
                                    text: {S: "Thanks for reading!"},
                                    timestamp: {N: "60"}
                                }}
                            ]},
                            subject: {S: "Great review"},
                            text: {S: "Appreciate the congrats"},
                            timestamp: {N: "0"},
                            upvotes: {N: "35"}
                        }}
                    ]},
                    subtitle: {S: "Does it live up to the hype?"},
                    title: {S: "Review of Rob Loblaw's Law Blog"}
                },
            });
    });
});
